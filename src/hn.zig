const std = @import("std");
const builtin = @import("builtin");
const http = std.http;
const zqlite = @import("zqlite");
const Allocator = std.mem.Allocator;
const getBindString = @import("./query_param.zig").getBindString;
const curl = @import("curl");

client: http.Client,
thread_pool: *std.Thread.Pool,
db: DB,

cache: struct {
    remover_task_active: bool = false,
    wg: std.Thread.WaitGroup = .{},
} = .{},

user: struct {
    username: []const u8,
    password: []const u8,
    cookie: ?[]const u8 = null,
} = .{
    .username = "",
    .password = "",
},

fetcher: *Fetcher,

const Fetcher = struct {
    multi: curl.Multi,
    ca_bundle: std.array_list.Managed(u8),

    blocker: std.Thread.ResetEvent = .{},
    result: std.AutoHashMapUnmanaged(*anyopaque, *Job.Shared) = .{},
    running: std.atomic.Value(bool) = .init(false),
    running_wg: std.Thread.WaitGroup = .{},

    queue: std.ArrayList(QueueItem) = .{},
    queue_mu: std.Thread.Mutex = .{},

    const QueueItem = struct {
        easy: curl.Easy,
        wg: *std.Thread.WaitGroup,
    };

    const Job = struct {
        easy: curl.Easy,
        buf: *std.Io.Writer.Allocating,
        shared: *Shared,

        owns_wg: bool,

        const Shared = struct {
            wg: *std.Thread.WaitGroup,
            failed: bool,
        };

        pub fn init(allocator: Allocator, wg_arg: ?*std.Thread.WaitGroup) !@This() {
            const wg = wg_arg orelse blk: {
                const w = try allocator.create(std.Thread.WaitGroup);
                w.* = .{};
                break :blk w;
            };

            const shared = try allocator.create(Shared);
            shared.* = .{
                .wg = wg,
                .failed = false,
            };

            const buf = try allocator.create(std.Io.Writer.Allocating);
            buf.* = .init(allocator);

            return .{
                .easy = try .init(.{}),
                .buf = buf,
                .owns_wg = wg_arg == null,
                .shared = shared,
            };
        }

        pub fn deinit(self: @This(), allocator: Allocator) void {
            if (self.owns_wg) allocator.destroy(self.shared.wg);
            self.easy.deinit();
            self.buf.deinit();

            allocator.destroy(self.shared);
            allocator.destroy(self.buf);
        }

        pub fn getResponse(self: @This()) !curl.Easy.Response {
            const easy_handle = self.easy.handle;
            var status_code: c_long = 0;

            try curl.checkCode(curl.libcurl.curl_easy_getinfo(
                easy_handle,
                curl.libcurl.CURLINFO_RESPONSE_CODE,
                &status_code,
            ));

            return .{
                .handle = easy_handle,
                .status_code = @intCast(status_code),
            };
        }

        pub fn wait(self: @This()) void {
            if (self.owns_wg)
                self.shared.wg.wait();
        }

        pub fn waitOutput(self: @This()) ![]const u8 {
            if (self.owns_wg) self.shared.wg.wait();
            if (self.shared.failed) return Error.FetchError;

            return self.buf.toOwnedSlice();
        }
    };

    fn deinit(self: *@This(), allocator: Allocator) void {
        if (self.running.load(.monotonic)) {
            self.stop();
            self.running_wg.wait();
        }

        self.result.deinit(allocator);
        self.multi.deinit();
        self.ca_bundle.deinit();
        self.queue.deinit(allocator);
    }

    fn stop(self: *@This()) void {
        self.running.store(false, .monotonic);
        self.multi.wakeup() catch {};
        self.blocker.set();
    }

    fn startBackground(self: *@This()) !void {
        self.running.store(true, .monotonic);
        const thread = try std.Thread.spawn(.{}, Fetcher.start, .{self});
        thread.detach();
    }

    fn start(self: *@This()) void {
        self.running_wg.start();
        self.running.store(true, .monotonic);
        defer {
            self.running.store(false, .monotonic);
            self.running_wg.finish();
        }

        while (self.running.load(.unordered)) {
            if (self.queue.items.len > 0) {
                self.queue_mu.lock();
                defer self.queue_mu.unlock();

                var limit: usize = 32;
                while (self.queue.pop()) |item| {
                    self.multi.addHandle(item.easy) catch |err| {
                        std.log.err("failed to add handle: {any}", .{err});
                        _ = self.result.remove(item.easy.handle);
                        continue;
                    };

                    limit -= 1;
                    if (limit == 0) break;
                }
            }

            const count = blk: {
                const c = self.multi.perform() catch |err| {
                    std.log.err("curl: failed to perform : {any}", .{err});
                    continue;
                };
                break :blk c;
            };

            if (count > 0) {
                _ = self.multi.poll(null, 100) catch {
                    continue;
                };
            }

            const info = self.multi.readInfo() catch |err| switch (err) {
                error.InfoReadExhausted => {
                    if (count == 0) {
                        self.blocker.timedWait(1e8 * 100) catch {};
                        self.blocker.reset();
                    }
                    continue;
                },
            };

            if (info.msg.msg != curl.libcurl.CURLMSG_DONE) continue;

            const easy_handle = info.msg.easy_handle.?;

            var status_code: c_long = 0;
            curl.checkCode(
                curl.libcurl.curl_easy_getinfo(
                    easy_handle,
                    curl.libcurl.CURLINFO_RESPONSE_CODE,
                    &status_code,
                ),
            ) catch |err| {
                std.log.err("curl: huh: {any}", .{err});
            };

            {
                self.queue_mu.lock();
                defer self.queue_mu.unlock();

                self.multi.removeHandle(easy_handle) catch |err| {
                    std.log.err("curl: failed to remove handle: {any}", .{err});
                };
            }

            if (self.result.get(easy_handle)) |item| {
                item.wg.finish();
                _ = curl.checkCode(info.msg.data.result) catch |err| {
                    std.debug.print("request failed: {any}\n", .{err});
                    item.failed = true;
                };
                _ = self.result.remove(easy_handle);
            }
        }
    }

    // Caller must deinit returned value
    fn submit(self: *@This(), allocator: Allocator, args: struct {
        url: [:0]const u8,
        method: curl.Easy.Method = .GET,
        body: ?[]const u8 = null,
        headers: ?curl.Easy.Headers = null,
        wg: ?*std.Thread.WaitGroup = null,
    }) !Job {
        if (!self.running.load(.unordered)) {
            @panic("fetcher is not running, cannot submit job");
        }

        var job: Fetcher.Job = try .init(allocator, args.wg);
        job.easy.ca_bundle = self.ca_bundle;

        job.shared.wg.start();
        errdefer job.shared.wg.finish();

        try job.easy.setUrl(args.url);
        try job.easy.setMethod(args.method);
        try job.easy.setWriter(&job.buf.writer);

        if (args.headers) |headers|
            try job.easy.setHeaders(headers);
        if (args.body) |body|
            try job.easy.setPostFields(body);

        {
            self.queue_mu.lock();
            defer self.queue_mu.unlock();

            try self.result.put(allocator, job.easy.handle, job.shared);
            errdefer _ = self.result.remove(job.easy.handle);

            try self.queue.append(allocator, .{
                .easy = job.easy,
                .wg = job.shared.wg,
            });
        }

        self.multi.wakeup() catch {};
        self.blocker.set();

        return job;
    }
};

pub const Error = error{ FetchError, InvalidCacheFile };

pub const ItemID = u64;

pub const ReplyMap = std.AutoHashMap(ItemID, *std.ArrayList(ItemID));

const Self = @This();
const String = []const u8;

const sec_to_nano: i128 = 1e9;

pub const Feed = enum {
    top_stories,
    ask_hn,
};

pub const Item = struct {
    // NOTE: make sure all fields have default values,
    // otherwise the zon parser will throw a fit when parsing
    by: String = "",
    id: ItemID = 0,
    score: ItemID = 0,
    title: String = "",
    url: ?String = null,
    descendants: usize = 0,
    kids: []const ItemID = &.{},
    time: i64 = 0,
    type: String = "",
    text: String = "",
    deleted: bool = false,
    dead: bool = false,
    parent: ?ItemID = null,
    thread_id: ?ItemID = null,

    depth: u8 = 0,
    inserted: i64 = 0,
    duped: bool = false,

    // Caller must free Item.free() afterwards
    pub fn dupe(self: Item, allocator: Allocator) !Item {
        const by = try allocator.dupe(u8, self.by);
        errdefer allocator.free(by);

        const title = try allocator.dupe(u8, self.title);
        errdefer allocator.free(title);

        const url: ?[]const u8 = if (self.url) |url|
            try allocator.dupe(u8, url)
        else
            null;
        errdefer if (url) |u| allocator.free(u);

        const kids = try allocator.dupe(ItemID, self.kids);
        errdefer allocator.free(kids);

        const itemType = try allocator.dupe(u8, self.type);
        errdefer allocator.free(itemType);

        const text = try allocator.dupe(u8, self.text);
        errdefer allocator.free(text);

        return .{
            .by = by,
            .id = self.id,
            .score = self.score,
            .title = title,
            .url = url,
            .descendants = self.descendants,
            .kids = kids,
            .time = self.time,
            .type = itemType,

            .deleted = self.deleted,
            .text = text,
            .dead = self.dead,
            .parent = self.parent,

            .thread_id = self.thread_id,

            .depth = self.depth,
            .inserted = self.inserted,

            .duped = true,
        };
    }

    pub fn free(self: Item, allocator: Allocator) void {
        allocator.free(self.by);
        allocator.free(self.title);
        if (self.url) |url| allocator.free(url);
        allocator.free(self.kids);
        allocator.free(self.type);
        allocator.free(self.text);
    }
};

pub fn init(allocator: Allocator) !Self {
    const client: http.Client = .{ .allocator = allocator };
    const n_jobs = @min(32, (std.Thread.getCpuCount() catch 1) * 8);

    const tpool: *std.Thread.Pool = try allocator.create(std.Thread.Pool);
    try tpool.init(.{
        .allocator = allocator,
        .n_jobs = n_jobs,
    });
    errdefer {
        tpool.deinit();
        allocator.destroy(tpool);
    }

    const db: DB = try .init(allocator, n_jobs);
    errdefer db.deinit();

    try db.createDatabase();

    const fetcher = try allocator.create(Fetcher);
    fetcher.* = .{
        .multi = try .init(),
        .ca_bundle = try curl.allocCABundle(allocator),
    };
    try fetcher.startBackground();

    return .{
        .client = client,
        .thread_pool = tpool,
        .db = db,
        .fetcher = fetcher,
    };
}

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.fetcher.deinit(allocator);
    self.client.deinit();
    self.thread_pool.deinit();
    self.db.deinit();

    allocator.destroy(self.thread_pool);
    allocator.destroy(self.fetcher);
}

pub fn waitBackgroundTasks(self: *Self) void {
    self.thread_pool.waitAndWork(&self.cache.wg);
}

// Caller must free returned string
pub fn fetch(self: *Self, allocator: Allocator, url: [:0]const u8) ![]const u8 {
    std.log.debug("fetching url {s}", .{url});
    defer std.log.debug("fetched url {s}", .{url});

    var job = try self.fetcher.submit(allocator, .{
        .method = .GET,
        .url = url,
    });
    defer job.deinit(allocator);

    job.wait();
    if (job.shared.failed) return Error.FetchError;

    return try job.buf.toOwnedSlice();
}

pub fn fetchAsync(
    self: *Self,
    allocator: Allocator,
    url: [:0]const u8,
    options: struct {
        wg: ?*std.Thread.WaitGroup = null,
    },
) !Fetcher.Job {
    std.log.debug("fetching url {s}", .{url});
    defer std.log.debug("fetched url {s}", .{url});

    const job = try self.fetcher.submit(allocator, .{
        .method = .GET,
        .url = url,
        .wg = options.wg,
    });

    return job;
}

// Caller must free returned slice
pub fn fetchTopStoryIDs(
    self: *Self,
    allocator: Allocator,
    limit: usize,
    offset: usize,
) ![]const ItemID {
    if (try self.db.readFeed(allocator, .top_stories, limit, offset)) |ids| {
        return ids;
    }

    const json_str = try self.fetch(allocator, "https://hacker-news.firebaseio.com/v0/topstories.json");
    defer allocator.free(json_str);

    const parsed = try std.json.parseFromSlice([]const u64, allocator, json_str, .{});
    defer parsed.deinit();

    try self.db.insertFeed(.top_stories, parsed.value);

    const i = @min(offset, parsed.value.len);
    const j = @min(i + limit, parsed.value.len);
    const ids = parsed.value[i..j];

    const result = try allocator.alloc(ItemID, ids.len);
    @memcpy(result, ids);

    return result;
}

const PromisedItem = struct {
    job: Fetcher.Job,
    url: [:0]const u8,

    pub fn deinit(self: @This(), allocator: Allocator) void {
        self.job.deinit(allocator);
        allocator.free(self.url);
    }

    pub fn await(self: @This(), allocator: Allocator) !Item {
        const json_str = try self.job.waitOutput();
        defer allocator.free(json_str);
        return parseItem(allocator, json_str);
    }
};

pub fn fetchItemAsync(self: *Self, allocator: Allocator, id: ItemID, options: struct {
    wg: ?*std.Thread.WaitGroup = null,
}) !PromisedItem {
    const url = try std.fmt.allocPrintSentinel(allocator, "https://hacker-news.firebaseio.com/v0/item/{d}.json", .{
        id,
    }, 0);

    return .{
        .url = url,
        .job = try self.fetchAsync(allocator, url, .{ .wg = options.wg }),
    };
}

fn parseItem(allocator: Allocator, json_str: []const u8) !Item {
    const parsed = try std.json.parseFromSlice(Item, allocator, json_str, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const item = try parsed.value.dupe(allocator);
    errdefer item.free(allocator);

    std.sort.block(ItemID, @constCast(item.kids), {}, compareID);
    return item;
}

// Caller must call Item.free() afterwards
pub fn fetchItem(self: *Self, allocator: Allocator, id: ItemID) !Item {
    const url = try std.fmt.allocPrintSentinel(allocator, "https://hacker-news.firebaseio.com/v0/item/{d}.json", .{
        id,
    }, 0);
    defer allocator.free(url);

    const json_str = try self.fetch(allocator, url);
    defer allocator.free(json_str);

    return parseItem(allocator, json_str);
}

// Caller must call Item.free() for each item and the free the slice itself
pub fn fetchAllItems(
    self: *Self,
    allocator: Allocator,
    ids: []const ItemID,
    options: struct {
        write_cache: bool,
        write_cache_wg: ?*std.Thread.WaitGroup = null,
    },
) ![]const Item {
    const cached_items = try self.db.readItems(allocator, ids);
    if (cached_items) |items| {
        return items;
    }

    const items: []Item = try allocator.alloc(Item, ids.len);
    errdefer {
        for (items) |item| {
            if (item.duped) item.free(allocator);
        }
        allocator.free(items);
    }

    var wg: std.Thread.WaitGroup = .{};

    var created_jobs: usize = 0;
    var jobs: []PromisedItem = try allocator.alloc(PromisedItem, ids.len);
    defer {
        for (0..created_jobs) |i| {
            jobs[i].deinit(allocator);
        }
        allocator.free(jobs);
    }

    for (ids, 0..) |id, i| {
        jobs[i] = try self.fetchItemAsync(allocator, id, .{ .wg = &wg });
        created_jobs += 1;
    }

    wg.wait();

    for (jobs, 0..) |*job, i| {
        items[i] = job.await(allocator) catch |err| {
            switch (err) {
                Error.FetchError => std.log.err("failed to fetch item: {d}", .{ids[i]}),
                else => {},
            }
            return err;
        };
    }

    if (options.write_cache) {
        if (options.write_cache_wg) |w| {
            self.thread_pool.spawnWg(w, struct {
                fn _(hn: *Self, data: []Item) void {
                    hn.db.insertItems(data) catch |err| {
                        std.log.err("failed to cache items: {any}", .{err});
                    };
                }
            }._, .{ self, items });
        } else {
            try self.db.insertItems(items);
        }
    }

    return items;
}

const AlgoliaItem = struct {
    id: ItemID = 0,
    parent_id: ?ItemID = null,
    points: ?usize = null,
    author: ?[]const u8 = null,
    created_at_i: i64 = 0,
    type: []const u8 = "",
    title: ?[]const u8 = null,
    text: ?[]const u8 = null,
    url: ?[]const u8 = null,
    story_id: ?ItemID = null,

    children: []const AlgoliaItem = &.{},
};

// Caller must call Item.free() for each item and the free the slice itself
/// Uses the official Algolia API. This should be preferred since it only
/// makes one HTTP request.
pub fn fetchThreadAlgolia(self: *Self, allocator: Allocator, opID: ItemID) ![]Item {
    std.log.debug("fetching thread {d} from algolia", .{opID});
    errdefer std.log.debug("failed to fetch thread {d} from algolia", .{opID});

    const url = try std.fmt.allocPrintSentinel(
        allocator,
        "https://hn.algolia.com/api/v1/items/{d}",
        .{opID},
        0,
    );
    defer allocator.free(url);

    const json_str = try self.fetch(allocator, url);
    defer allocator.free(json_str);

    std.log.debug("parsing thread {d} response from algolia", .{opID});
    const parsed = try std.json.parseFromSlice(AlgoliaItem, allocator, json_str, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    std.log.debug("parsed thread {d} response from algolia:\n{s}...\n", .{ opID, json_str[0..32] });

    const countItems = struct {
        fn loop(aitem: *const AlgoliaItem) usize {
            var count: usize = 1;
            for (aitem.children) |child| count += loop(&child);
            return count;
        }
    }.loop;

    const collect = struct {
        fn loop(
            gpa: Allocator,
            thread_id: ItemID,
            items: *std.ArrayList(Item),
            aitem: *const AlgoliaItem,
        ) !usize {
            var kids = try gpa.alloc(ItemID, aitem.children.len);
            errdefer gpa.free(kids);

            var item = try (Item{
                .id = aitem.id,
                .url = aitem.url,
                .parent = aitem.parent_id,
                .score = aitem.points orelse 0,
                .by = aitem.author orelse "",
                .time = aitem.created_at_i,
                .type = aitem.type,
                .title = aitem.title orelse "",
                .text = aitem.text orelse "",
                .kids = &.{},
                .thread_id = aitem.story_id,
            }).dupe(gpa);
            errdefer item.free(gpa);

            const item_idx = items.items.len;
            items.appendAssumeCapacity(item);

            var comment_count: usize = 1;
            for (aitem.children, 0..) |child, i| {
                kids[i] = child.id;

                const sub_item = @constCast(&child);
                comment_count += try loop(gpa, thread_id, items, sub_item);
            }

            std.sort.block(ItemID, @constCast(kids), {}, compareID);

            var p = &items.items[item_idx];
            p.kids = kids;
            p.descendants = comment_count;

            return comment_count;
        }
    }.loop;

    const num_items = countItems(&parsed.value);
    var result: std.ArrayList(Item) = try .initCapacity(allocator, num_items);

    defer result.deinit(allocator);
    errdefer for (result.items) |item| item.free(allocator);

    _ = try collect(allocator, opID, &result, &parsed.value);
    std.log.debug("fetched thread {d} from algolia : got {d} items ", .{ opID, result.items.len });

    return try result.toOwnedSlice(allocator);
}

// /item?id=123
// /item/discussion?id=123&tid=456

/// Caller must call Item.free() for each item and the free the slice itself
pub fn fetchLineage(_: *Self, allocator: Allocator, item_id: ItemID) ![]Item {
    std.log.debug("fetching discussion {d} from HN official API", .{item_id});
    var result: std.ArrayList(Item) = .{};
    defer result.deinit(allocator);
    errdefer {
        for (result.items) |item| {
            item.free(allocator);
        }
    }

    var current = item_id;
    while (true) {
        const item = try fetchItem(allocator, current);
        if (item.parent != null)
            try result.append(allocator, item);
        current = item.parent orelse break;
    }

    const items: []Item = result.toOwnedSlice(allocator);
    std.mem.reverse(Item, items);

    for (items[1..], 0..) |*item, i| {
        _ = i;
        allocator.free(item.kids);
        item.kids = &.{}; // no need to show replies in this case
    }

    return items;
}

/// Caller must call Item.free() for each item and the free the slice itself
/// Uses the official HN API. About at least two times slower compared
/// to fetchThreadAlgolia() since this once makes recursive HTTP requests
/// for each item.
pub fn fetchThreadOfficial(self: *Self, allocator: Allocator, opID: ItemID) ![]Item {
    std.log.debug("fetching thread {d} from HN official API", .{opID});
    var result: std.ArrayList(Item) = .{};
    defer result.deinit(allocator);
    errdefer {
        for (result.items) |item| {
            item.free(allocator);
        }
    }

    var queue: std.ArrayList(ItemID) = .{};
    defer queue.deinit(allocator);

    try queue.append(allocator, opID);

    while (queue.items.len > 0) {
        const items = try self.fetchAllItems(allocator, queue.items, .{ .write_cache = false });
        defer allocator.free(items);
        queue.clearRetainingCapacity();

        try result.appendSlice(allocator, items);
        for (items) |item| {
            try queue.appendSlice(allocator, item.kids);
        }
    }

    return result.toOwnedSlice(allocator);
}

// Caller must call Item.free() for each item and the free the slice itself
// Uses the Algolia API, then falls back to official HN API if it fails.
pub fn fetchDiscussion(
    self: *Self,
    allocator: Allocator,
    thread_id: ItemID,
    item_id: ItemID,
) ![]const Item {
    std.log.debug("fetching discussion {d}", .{item_id});
    errdefer std.log.debug("failed to discussion thread {d}", .{item_id});

    var cached_items = try self.db.readDiscussion(allocator, item_id);
    if (cached_items) |items| {
        std.log.debug("fetched cached discussion {d}: found {d} items", .{ item_id, items.len });
        return items;
    }

    // thread is not yet loaded, try to fetch it
    // TODO: ideally I should use the items from fetchThread
    // but I'll read again from db anyways for lazy reasons
    const temp_items: []const Item = try self.fetchThread(allocator, thread_id, .{});
    freeItems(allocator, temp_items);

    // try again
    cached_items = try self.db.readDiscussion(allocator, item_id);
    if (cached_items) |items| {
        std.log.debug("fetched cached discussion {d}: found {d} items", .{ item_id, items.len });
        return items;
    }

    // still didn't find any, oh well
    return &.{};
}

// Caller must call Item.free() for each item and the free the slice itself
// Uses the Algolia API, then falls back to official HN API if it fails.
pub fn fetchThread(
    self: *Self,
    allocator: Allocator,
    opID: ItemID,
    options: struct {
        wg: ?*std.Thread.WaitGroup = null,
    },
) ![]const Item {
    std.log.debug("fetching thread {d}", .{opID});
    errdefer std.log.debug("failed to fetch thread {d}", .{opID});

    const cached_items = try self.db.readThread(allocator, opID);
    if (cached_items) |items| {
        std.log.debug("fetched cached thread {d}: found {d} items", .{ opID, items.len });
        return items;
    }

    const items = blk: {
        break :blk self.fetchThreadAlgolia(allocator, opID) catch |err| {
            return switch (err) {
                Error.FetchError => break :blk try self.fetchThreadOfficial(allocator, opID),
                else => return err,
            };
        };
    };

    std.sort.block(Item, @constCast(items), {}, compareItem);

    if (items.len > 0 and items[0].id < opID) {
        // handle case where op doesn't have the lowest ID,
        // probably happens when dang moves an older post into the newer thread
        const op_index = std.sort.binarySearch(Item, items, opID, struct {
            fn _(id: ItemID, item: Item) std.math.Order {
                return std.math.order(id, item.id);
            }
        }._);
        if (op_index) |i| {
            const temp = items[0];
            items[0] = items[i];
            items[i] = temp;
        }
    }

    std.log.debug("fetched thread {d}: got {d} items", .{ opID, items.len });

    if (options.wg) |wg| {
        self.thread_pool.spawnWg(wg, struct {
            fn _(hn: *Self, op_id: ItemID, data: []Item) void {
                hn.db.insertItems(data) catch |err| {
                    std.log.err("failed to cache thread: {d}: {any}", .{ op_id, err });
                };
            }
        }._, .{ self, opID, items });
    } else {
        try self.db.insertItems(items);
    }

    return items;
}

pub fn fillItems(
    arena: Allocator,
    items: []Item,
    dest: *std.AutoHashMapUnmanaged(ItemID, Item),
) !void {
    for (items, 0..) |item, i| {
        var copy = item;

        if (item.parent) |parent_id| if (dest.get(parent_id)) |parent| {
            copy.depth = parent.depth + 1;
            items[i].depth = copy.depth;
        };
        try dest.put(arena, item.id, copy);
    }
}

fn compareID(_: void, a: ItemID, b: ItemID) bool {
    return a < b;
}
fn compareItem(_: void, a: Item, b: Item) bool {
    return a.id < b.id;
}

// Frees both slice and the individual items.
pub fn freeItems(allocator: Allocator, items: []const Item) void {
    for (items) |item| item.free(allocator);
    allocator.free(items);
}

fn dupeItems(allocator: Allocator, items: []const Item) ![]const Item {
    const copy = try allocator.alloc(Item, items.len);
    for (0..items.len) |i| {
        copy[i] = try items[i].dupe(allocator);
    }
    return copy;
}

const DB = struct {
    pool: *zqlite.Pool,

    const expiration_sec: i128 = if (builtin.mode == .Debug) 8 * 60 * 60 else 15 * 60;
    const items_limit: usize = 1e4 * 50;
    const db_filename = "zlacker.db";

    pub fn init(allocator: Allocator, pool_size: usize) !@This() {
        return .{
            .pool = try .init(allocator, .{
                .path = db_filename,
                .size = pool_size,
            }),
        };
    }

    pub fn deinit(self: @This()) void {
        self.pool.deinit();
    }

    pub fn insertFeed(self: @This(), feed: Feed, ids: []const ItemID) !void {
        const conn = self.pool.acquire();
        defer conn.release();

        try conn.execNoArgs("PRAGMA journal_mode=WAL2");
        try conn.transaction();
        defer conn.commit() catch unreachable;
        errdefer conn.rollback();

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        const stmt = try conn.prepare(
            \\INSERT INTO hn_feed(feed, num, item_id, inserted) VALUES(?, ?, ?, ?);
        );
        defer stmt.deinit();

        const now = std.time.timestamp();
        for (ids, 0..) |id, index| {
            try stmt.reset();
            try stmt.bind(.{ @intFromEnum(feed), index, id, now });
            _ = try stmt.step();
        }
    }

    pub fn readFeed(
        self: @This(),
        allocator: Allocator,
        feed: Feed,
        limit: usize,
        offset: usize,
    ) !?[]const ItemID {
        const conn = self.pool.acquire();
        defer conn.release();

        errdefer std.log.err("failed to read feed: {s}", .{conn.lastError()});

        var result: std.ArrayList(ItemID) = .{};
        defer result.deinit(allocator);

        var rows = try conn.rows(
            \\SELECT item_id, inserted from hn_feed WHERE feed = ? ORDER BY num ASC LIMIT ? OFFSET ?;
        , .{ @intFromEnum(feed), limit, offset });
        defer rows.deinit();

        const now = std.time.timestamp();
        var count: usize = 0;
        while (rows.next()) |row| {
            const inserted = row.int(1);

            if (now - inserted >= expiration_sec) {
                return null;
            }

            try result.append(allocator, @intCast(row.int(0)));
            count += 1;
        }

        if (count == 0) return null;

        return try result.toOwnedSlice(allocator);
    }

    pub fn readThread(
        self: @This(),
        allocator: Allocator,
        op_id: ItemID,
    ) !?[]const Item {
        var result: std.ArrayList(Item) = .{};
        var abort = false;
        var op: ?Item = null;

        defer if (abort) {
            for (result.items) |item| {
                item.free(allocator);
            }
            result.deinit(allocator);
        };

        errdefer abort = true;

        const conn = self.pool.acquire();
        defer conn.release();

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        var rows = try conn.rows(
            \\WITH RECURSIVE
            \\    thread(
            \\            id, username, score, title, url, descendants, time, text,
            \\            deleted, dead, parent_id, thread_id, depth, inserted
            \\    ) AS (
            \\        SELECT
            \\            id,
            \\            username, score, title, url, descendants, time, text,
            \\            deleted, dead, parent_id, thread_id, depth, inserted
            \\        FROM hn_items WHERE id = ?
            \\        UNION
            \\        SELECT
            \\            t.id, t.username, t.score, t.title, t.url, t.descendants,
            \\            t.time, t.text, t.deleted, t.dead, t.parent_id, t.thread_id,
            \\            t.depth, t.inserted
            \\          FROM hn_items t JOIN thread
            \\          ON t.parent_id = thread.id
            \\    )
            \\SELECT * from thread ORDER BY id DESC;
        , .{op_id});
        defer rows.deinit();

        var reply_ids: std.AutoArrayHashMapUnmanaged(ItemID, *std.ArrayList(ItemID)) = .{};
        defer {
            for (reply_ids.values()) |ids| {
                ids.deinit(allocator);
                allocator.destroy(ids);
            }
            reply_ids.deinit(allocator);
        }

        const now = std.time.timestamp();
        while (rows.next()) |row| {
            var item = Item{
                .id = @intCast(row.int(0)),
                .by = row.text(1),
                .score = @intCast(row.int(2)),
                .title = row.text(3),
                .url = row.nullableText(4),
                .descendants = @intCast(row.int(5)),
                .time = @intCast(row.int(6)),
                .text = row.text(7),
                .deleted = row.int(8) != 0,
                .dead = row.int(9) != 0,
                .parent = if (row.nullableInt(10)) |n| @intCast(n) else null,
                .thread_id = if (row.nullableInt(11)) |n| @intCast(n) else null,
                .depth = @intCast(row.int(12)),
                .inserted = @intCast(row.int(13)),
            };

            if (now - item.inserted >= expiration_sec) {
                std.log.debug("thread cached expired: {d}", .{op_id});
                for (result.items) |entry| entry.free(allocator);
                result.deinit(allocator);
                return null;
            }

            if (reply_ids.get(item.id)) |ids| {
                item.kids = ids.items;
                std.sort.block(ItemID, @constCast(item.kids), {}, compareID);
            }

            if (item.parent) |parent_id| {
                if (reply_ids.get(parent_id)) |ids| {
                    try ids.append(allocator, item.id);
                } else {
                    var ids = try allocator.create(std.ArrayList(ItemID));
                    ids.* = .{};
                    try ids.append(allocator, item.id);
                    try reply_ids.put(allocator, parent_id, ids);
                }
            }

            item = try item.dupe(allocator);

            if (item.id == op_id) {
                // store op separately to make sure it's always
                // first since there cases
                // where op's id and time is less than other posts
                op = item;
            } else {
                errdefer item.free(allocator);
                try result.append(allocator, item);
            }
        }

        if (op) |item| try result.append(allocator, item);

        if (result.items.len == 0 or result.items.len < result.items[0].descendants) {
            abort = true;
            return null;
        }

        const items = try result.toOwnedSlice(allocator);
        std.mem.reverse(Item, items);

        return items;
    }

    pub fn readDiscussion(
        self: @This(),
        allocator: Allocator,
        item_id: ItemID,
    ) !?[]const Item {
        var result: std.ArrayList(Item) = .{};
        errdefer {
            for (result.items) |item| item.free(allocator);
            result.deinit(allocator);
        }

        const conn = self.pool.acquire();
        defer conn.release();

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        var rows = try conn.rows(
            \\WITH RECURSIVE
            \\    thread( id, username, score, title, url, descendants, time, text,
            \\            deleted, dead, parent_id, thread_id, depth, inserted
            \\    ) AS (
            \\        SELECT
            \\            id,
            \\            username, score, title, url, descendants, time, text,
            \\            deleted, dead, parent_id, thread_id, depth, inserted
            \\        FROM hn_items WHERE id = ?
            \\        UNION
            \\        SELECT
            \\            t.id, t.username, t.score, t.title, t.url, t.descendants, t.time, t.text,
            \\            t.deleted, t.dead, t.parent_id, t.thread_id, t.depth, t.inserted
            \\          FROM hn_items t JOIN thread
            \\          ON t.id = thread.parent_id
            \\    )
            \\SELECT * from thread ORDER BY id;
        , .{item_id});
        defer rows.deinit();

        const now = std.time.timestamp();
        while (rows.next()) |row| {
            var item = Item{
                .id = @intCast(row.int(0)),
                .by = row.text(1),
                .score = @intCast(row.int(2)),
                .title = row.text(3),
                .url = row.nullableText(4),
                .descendants = @intCast(row.int(5)),
                .time = @intCast(row.int(6)),
                .text = row.text(7),
                .deleted = row.int(8) != 0,
                .dead = row.int(9) != 0,
                .parent = if (row.nullableInt(10)) |n| @intCast(n) else null,
                .thread_id = if (row.nullableInt(11)) |n| @intCast(n) else null,
                .depth = @intCast(row.int(12)),
                .inserted = @intCast(row.int(13)),
            };

            if (now - item.inserted >= expiration_sec) {
                std.log.debug("discussion cached expired: {d}", .{item_id});
                for (result.items) |entry| entry.free(allocator);
                result.deinit(allocator);
                return null;
            }

            item = try item.dupe(allocator);
            errdefer item.free(allocator);

            try result.append(allocator, item);
        }

        if (result.items.len == 0) {
            return null;
        }

        return try result.toOwnedSlice(allocator);
    }

    pub fn readItems(
        self: @This(),
        allocator: Allocator,
        ids: []const ItemID,
    ) !?[]const Item {
        if (ids.len == 0) return &.{};

        var result: std.ArrayList(Item) = .{};
        errdefer {
            for (result.items) |item| item.free(allocator);
            result.deinit(allocator);
        }

        const conn = self.pool.acquire();
        defer conn.release();

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        const query = try std.fmt.allocPrint(allocator,
            \\SELECT
            \\  id,
            \\  username,
            \\  score,
            \\  title,
            \\  url,
            \\  descendants,
            \\  time,
            \\  text,
            \\  deleted,
            \\  dead,
            \\  parent_id,
            \\  thread_id,
            \\  depth,
            \\  inserted
            \\FROM hn_items
            \\WHERE id IN ({s})
        , .{getBindString(ids.len)});
        defer allocator.free(query);

        var rows: zqlite.Rows = .{ .stmt = try conn.prepare(query), .err = null };
        defer rows.deinit();
        for (ids, 0..) |id, i| try rows.stmt.bindValue(id, i);

        var invalidated = false;
        const now = std.time.timestamp();

        while (rows.next()) |row| {
            const item = Item{
                .id = @intCast(row.int(0)),
                .by = row.text(1),
                .score = @intCast(row.int(2)),
                .title = row.text(3),
                .url = row.nullableText(4),
                .descendants = @intCast(row.int(5)),
                .time = @intCast(row.int(6)),
                .text = row.text(7),
                .deleted = row.int(8) != 0,
                .dead = row.int(9) != 0,
                .parent = if (row.nullableInt(10)) |n| @intCast(n) else null,
                .thread_id = if (row.nullableInt(11)) |n| @intCast(n) else null,
                .depth = @intCast(row.int(12)),
                .inserted = @intCast(row.int(13)),
            };

            if (now - item.inserted >= expiration_sec) {
                invalidated = true;
                break;
            }

            try result.append(allocator, try item.dupe(allocator));
        }

        if (invalidated or result.items.len < ids.len) {
            for (result.items) |item| item.free(allocator);
            result.deinit(allocator);
            return null;
        }

        const items = try result.toOwnedSlice(allocator);
        return items;
    }

    fn readItem(
        self: @This(),
        allocator: Allocator,
        id: ItemID,
    ) !?Item {
        const conn = self.pool.acquire();
        defer conn.release();
        try conn.busyTimeout(1000);

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        const item_stmt = try conn.row(
            \\SELECT
            \\  id,
            \\  username,
            \\  score,
            \\  title,
            \\  url,
            \\  descendants,
            \\  time,
            \\  text,
            \\  deleted,
            \\  dead,
            \\  parent_id,
            \\  thread_id,
            \\  depth,
            \\  inserted,
            \\  (SELECT COUNT(*) FROM hn_item_replies
            \\    WHERE item_id = hn_items.id)
            \\FROM hn_items
            \\WHERE id = ?;
        , .{id}) orelse return null;
        defer item_stmt.deinit();

        var item = try (Item{
            .id = @intCast(item_stmt.int(0)),
            .by = item_stmt.text(1),
            .score = @intCast(item_stmt.int(2)),
            .title = item_stmt.text(3),
            .url = item_stmt.nullableText(4),
            .descendants = @intCast(item_stmt.int(5)),
            .time = @intCast(item_stmt.int(6)),
            .text = item_stmt.text(7),
            .deleted = item_stmt.int(8) != 0,
            .dead = item_stmt.int(9) != 0,
            .parent = if (item_stmt.nullableInt(10)) |n| @intCast(n) else null,
            .thread_id = if (item_stmt.nullableInt(11)) |n| @intCast(n) else null,
            .depth = @intCast(item_stmt.int(12)),
            .inserted = @intCast(item_stmt.int(13)),
        }).dupe(allocator);
        errdefer item.free(allocator);

        {
            const num_replies: usize = @intCast(item_stmt.int(14));
            if (num_replies > 0) {
                const replies: []ItemID = try allocator.alloc(ItemID, num_replies);
                errdefer allocator.free(replies);

                var rows = try conn.rows(
                    \\SELECT reply_id FROM hn_item_replies
                    \\WHERE item_id = ?;
                , .{item.id});

                var i: usize = 0;
                while (rows.next()) |row| {
                    replies[i] = @intCast(row.int(0));
                    i += 1;
                }
                item.kids = replies;
            }
        }

        return item;
    }

    pub fn insertItems(self: @This(), items: []Item) !void {
        const conn = self.pool.acquire();
        defer conn.release();
        try conn.busyTimeout(1000 * 10);

        try conn.execNoArgs("PRAGMA journal_mode=WAL2");
        try conn.transaction();
        defer conn.commit() catch unreachable;
        errdefer conn.rollback();

        const insert_stmt = try conn.prepare(
            \\INSERT INTO hn_items(
            \\  id,
            \\  username,
            \\  score,
            \\  title,
            \\  url,
            \\  descendants,
            \\  time,
            \\  text,
            \\  deleted,
            \\  dead,
            \\  parent_id,
            \\  thread_id,
            \\  depth,
            \\  inserted
            \\) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            \\ON CONFLICT DO
            \\UPDATE SET
            \\ score=excluded.score,
            \\ title=excluded.title,
            \\ url=excluded.url,
            \\ descendants=excluded.descendants,
            \\ text=excluded.text ,
            \\ deleted=excluded.deleted ,
            \\ dead=excluded.dead ,
            \\ depth=excluded.depth ,
            \\ inserted=excluded.inserted;
        );
        defer insert_stmt.deinit();

        const insert_reply_stmt = try conn.prepare(
            \\INSERT INTO hn_item_replies(item_id, reply_id)
            \\VALUES(?, ?);
        );
        defer insert_reply_stmt.deinit();

        errdefer {
            std.log.err("failed to insert items: {s}", .{conn.lastError()});
        }

        for (items) |item| {
            try insert_stmt.reset();
            try insert_stmt.bind(.{
                item.id,
                item.by,
                item.score,
                item.title,
                item.url,
                item.descendants,
                item.time,
                item.text,
                item.deleted,
                item.dead,
                item.parent,
                item.thread_id,
                item.dead,
                std.time.timestamp(),
            });
            _ = try insert_stmt.step();

            for (item.kids) |reply_id| {
                try insert_reply_stmt.reset();
                try insert_reply_stmt.bind(.{ item.id, reply_id });
                _ = try insert_reply_stmt.step();
            }
        }
    }

    pub fn createDatabase(self: @This()) !void {
        const conn = self.pool.acquire();
        defer conn.release();
        errdefer {
            std.log.err("failed to create database: {s}", .{conn.lastError()});
        }

        try conn.execNoArgs(
            \\CREATE TABLE IF NOT EXISTS hn_feed(
            \\  feed INTEGER,
            \\  num INTEGER,
            \\  item_id INTEGER,
            \\  inserted INTEGER,
            \\
            \\  FOREIGN KEY (item_id) references hn_items(id),
            \\  UNIQUE (feed, num) ON CONFLICT REPLACE
            \\);
            \\
            \\CREATE TABLE IF NOT EXISTS hn_item_replies(
            \\  item_id INTEGER,
            \\  reply_id INTEGER,
            \\
            \\  FOREIGN KEY (item_id) references hn_items(id)
            \\    ON DELETE CASCADE,
            \\  UNIQUE (item_id, reply_id) ON CONFLICT IGNORE
            \\);
            \\
            \\CREATE TABLE IF NOT EXISTS hn_items(
            \\  id INTEGER PRIMARY KEY,
            \\  username VARCHAR(16),
            \\  score INTEGER,
            \\  title VARCHAR(100),
            \\  url TEXT,
            \\  descendants INTEGER,
            \\  time INTEGER,
            \\  text TEXT,
            \\  deleted TINYINT,
            \\  dead TINYINT,
            \\  parent_id INTEGER,
            \\  thread_id INTEGER,
            \\  depth INTEGER,
            \\  inserted INTEGER,
            \\
            \\  FOREIGN KEY (parent_id) references hn_items(id)
            \\  FOREIGN KEY (thread_id) references hn_items(id)
            \\);
        );
    }

    pub fn startCleaner(self: @This()) !void {
        const Instance = @This();
        const thread = try std.Thread.spawn(.{}, struct {
            fn loop(db: Instance) void {
                while (true) {
                    std.log.debug("removing old items", .{});
                    db.removePastLimit() catch |err| {
                        std.log.err("failed to remove old items: {any}", .{err});
                    };
                    std.Thread.sleep(1e9 * 60 * 30);
                }
            }
        }.loop, .{self});

        thread.detach();
    }

    pub fn removePastLimit(self: @This()) !void {
        const conn = self.pool.acquire();
        defer conn.release();
        errdefer std.log.err("{s}", .{conn.lastError()});

        const row = try conn.row(
            \\ SELECT inserted FROM hn_items
            \\ ORDER BY inserted DESC
            \\ LIMIT ? OFFSET ?;
        , .{ items_limit, items_limit }) orelse return;
        defer row.deinit();

        const inserted: ItemID = @intCast(row.int(0));

        try conn.exec(
            \\ DELETE FROM hn_items
            \\ WHERE inserted < ?;
        , .{inserted});
    }
};

fn printStackTrace(trace_arg: ?*std.builtin.StackTrace) void {
    const trace = trace_arg orelse return;
    var stdout = std.fs.File.stdout().writer(&.{});
    const debug_info = std.debug.getSelfDebugInfo() catch return;
    std.debug.writeStackTrace(
        trace.*,
        &stdout.interface,
        debug_info,
        .no_color,
    ) catch {};
}

const test_output = @import("build_options").test_output;

test "fetch 1" {
    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = std.testing.allocator,
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);
    defer hn.deinit(allocator);

    const item = try hn.fetchItem(allocator, 314693);
    defer item.free(allocator);

    const t = std.testing;
    try t.expectEqualStrings("Reading source code: The rise of F#", item.title);
    try t.expectEqualStrings("bdfh42", item.by);
    try t.expectEqual(314693, item.id);
    try t.expectEqual(1222333484, item.time);

    hn.waitBackgroundTasks();
}

test "fetch:index" {
    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = std.testing.allocator,
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);
    defer hn.deinit(allocator);

    const story_ids = try hn.fetchTopStoryIDs(allocator, 30, 0);
    defer allocator.free(story_ids);

    const items: []const Item = try hn.fetchAllItems(allocator, story_ids[0..1], .{ .write_cache = false });
    defer {
        for (items) |item| item.free(allocator);
        allocator.free(items);
    }

    const t = std.testing;
    try t.expectEqual(1, items.len);
    try t.expect(items[0].title.len > 0);
    try t.expect(items[0].by.len > 0);
    try t.expect(items[0].id > 0);
    try t.expect(items[0].time > 0);

    hn.waitBackgroundTasks();
}

test fetchThread {
    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = std.testing.allocator,
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);
    defer hn.deinit(allocator);

    const items = try hn.fetchThread(allocator, 1727731, .{});
    defer freeItems(allocator, items);

    var count: usize = 0;
    if (test_output) {
        for (items) |item| {
            std.debug.print("> {d} {s} : {s} : {d}\n{s}\n--------\n", .{
                item.id,
                item.title,
                item.by,
                item.kids.len,
                item.text,
            });
            count += 1;
        }
        std.debug.print("total num items: {d}\n", .{count});
    }
}

test fetchDiscussion {
    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = std.testing.allocator,
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);
    defer hn.deinit(allocator);

    const items = try hn.fetchDiscussion(
        allocator,
        4558429,
        4558521,
    );
    defer freeItems(allocator, items);

    var count: usize = 0;
    if (test_output) {
        for (items) |item| {
            std.debug.print("> {d} {s} : {s} : {d}\n{s}\n--------\n", .{
                item.id,
                item.title,
                item.by,
                item.kids.len,
                item.text,
            });
            count += 1;
        }
        std.debug.print("total num items: {d}\n", .{count});
    }
}

fn percentEncodeStr(allocator: Allocator, str: []const u8) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    try percentEncode(&buf.writer, str);
    return buf.toOwnedSlice();
}

fn percentEncode(w: *std.Io.Writer, str: []const u8) !void {
    for (str) |ch| {
        const esc = switch (ch) {
            ':' => "%3A",
            '/' => "%2F",
            '?' => "%3F",
            '#' => "%23",
            '[' => "%5B",
            ']' => "%5D",
            '@' => "%40",
            '!' => "%21",
            '$' => "%24",
            '&' => "%26",
            '\'' => "%27",
            '(' => "%28",
            ')' => "%29",
            '*' => "%2A",
            '+' => "%2B",
            ',' => "%2C",
            ';' => "%3B",
            '=' => "%3D",
            '%' => "%25",
            ' ' => "+",
            else => null,
        };
        if (esc) |c|
            try w.writeAll(c)
        else
            try w.writeByte(ch);
    }
}
