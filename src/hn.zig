const std = @import("std");
const builtin = @import("builtin");
const http = std.http;
const Allocator = std.mem.Allocator;

client: http.Client,
thread_pool: *std.Thread.Pool,

cache: struct {
    remover_task_active: bool = false,
    expiration_sec: i128 = if (builtin.mode == .Debug) 8 * 60 * 60 else 1 * 60,
    wg: std.Thread.WaitGroup = .{},
} = .{},

pub const Error = error{ FetchError, InvalidCacheFile };

pub const ItemID = u64;

pub const ReplyMap = std.AutoHashMap(ItemID, *std.ArrayList(ItemID));

const Self = @This();
const String = []const u8;

const sec_to_nano: i128 = 1e9;

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
    time: ItemID = 0,
    type: String = "",
    text: String = "",
    deleted: bool = false,
    dead: bool = false,
    parent: ?ItemID = null,
    sibling_num: ItemID = 0,
    thread_id: ?ItemID = null,

    depth: u8 = 0,

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

            .sibling_num = self.sibling_num,
            .thread_id = self.thread_id,
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

    const tpool: *std.Thread.Pool = try allocator.create(std.Thread.Pool);
    try tpool.init(.{
        .allocator = allocator,
        .n_jobs = (std.Thread.getCpuCount() catch 1) * 2,
    });

    return .{
        .client = client,
        .thread_pool = tpool,
    };
}

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.client.deinit();
    self.thread_pool.deinit();
    allocator.destroy(self.thread_pool);
}

pub fn waitBackgroundTasks(self: *Self) void {
    self.thread_pool.waitAndWork(&self.cache.wg);
}

pub fn startStaleCacheRemover(self: *Self, allocator: Allocator) !void {
    const task = struct {
        fn _(alloc: Allocator, expiration: i138) void {
            while (true) {
                defer std.Thread.sleep(60 * 1 * sec_to_nano);
                removeStaleCacheFiles(alloc, expiration) catch |err| {
                    std.debug.print("an error occured while removing caches: {any}\n", .{err});
                };
            }
        }
    }._;

    const thread = try std.Thread.spawn(.{}, task, .{
        allocator,
        self.cache.expiration_sec * sec_to_nano,
    });
    thread.detach();
}

const dirname = ".zlacker-cache";
pub fn removeStaleCacheFiles(allocator: Allocator, expiration: i138) !void {
    std.log.debug("removing stale cache files", .{});
    var dir = try std.fs.cwd().openDir(dirname, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        defer std.Thread.sleep(1 * sec_to_nano);

        const filename = try std.fs.path.join(allocator, &.{ dirname, entry.name });
        defer allocator.free(filename);

        const stat = std.fs.cwd().statFile(filename) catch continue;
        const elapsed = std.time.nanoTimestamp() - stat.mtime;
        if (elapsed > expiration) {
            std.fs.cwd().deleteFile(filename) catch |err| {
                std.log.debug("failed to delete cache file : {any}\n", .{err});
                continue;
            };
            std.log.debug("-> cache file deleted: {s}\n", .{filename});
        }
    }
}

// Caller must free returned string
pub fn fetch(self: *Self, arena: Allocator, url: []const u8) ![]const u8 {
    // TODO: okay, I thought it's was a deadlock on a
    // mutex that's causing it to hang,
    // but it seems to happen here.
    // Maybe a bug in the zig http client,
    // I could probably add a timeout.
    // Well actually it's also possible to be blocking
    // on allocation which is protected by a mutex.
    // It seems to happen though when I leave the
    // program running after a while.
    // Maybe it's the connection pool too?

    std.log.debug("fetching url {s}", .{url});
    defer std.log.debug("fetched url {s}", .{url});
    var buf: std.Io.Writer.Allocating = .init(arena);
    defer buf.deinit();

    const result = try self.client.fetch(.{
        .method = .GET,
        .location = .{ .url = url },
        .response_writer = &buf.writer,
        .keep_alive = false,
    });

    if (result.status != .ok) return Error.FetchError;

    return buf.toOwnedSlice();
}

// Caller must free returned slice
pub fn fetchTopStoryIDs(self: *Self, allocator: Allocator) ![]const ItemID {
    if (try self.loadFeedCache(allocator, "topstories")) |ids| {
        return ids;
    }

    const json_str = try self.fetch(allocator, "https://hacker-news.firebaseio.com/v0/topstories.json");
    defer allocator.free(json_str);

    const parsed = try std.json.parseFromSlice([]const u64, allocator, json_str, .{});
    defer parsed.deinit();

    const ids = try allocator.dupe(ItemID, parsed.value);

    cacheFeed(allocator, "topstories", ids) catch |err| {
        std.debug.print("failed to cache feed {s} : {any}", .{ "topstories", err });
        printStackTrace(@errorReturnTrace());
    };

    return ids;
}

// Caller must call Item.free() afterwards
pub fn fetchItem(self: *Self, allocator: Allocator, id: ItemID) !Item {
    if (try self.loadItemCache(allocator, id)) |item| {
        return item;
    }

    const url = try std.fmt.allocPrint(allocator, "https://hacker-news.firebaseio.com/v0/item/{d}.json", .{
        id,
    });
    defer allocator.free(url);

    const json_str = try self.fetch(allocator, url);
    defer allocator.free(json_str);

    const parsed = try std.json.parseFromSlice(Item, allocator, json_str, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const item = try parsed.value.dupe(allocator);

    std.sort.block(ItemID, @constCast(item.kids), {}, struct {
        fn _(_: void, a: ItemID, b: ItemID) bool {
            return a < b;
        }
    }._);

    cacheItem(allocator, item) catch |err| {
        std.debug.print("failed to cache item: {any}", .{err});
        printStackTrace(@errorReturnTrace());
    };

    return item;
}

// Caller must call Item.free() for each item and the free the slice itself
pub fn fetchAllItems(self: *Self, allocator: Allocator, ids: []const ItemID) ![]const Item {
    switch (ids.len) {
        0 => return allocator.dupe(Item, &.{}),
        1 => {
            const item = try self.fetchItem(allocator, ids[0]);
            return allocator.dupe(Item, &.{item});
        },
        else => {},
    }

    const items: []Item = try allocator.alloc(Item, ids.len);

    var wg: std.Thread.WaitGroup = .{};

    const SpawnArgs = struct {
        id: ItemID,
        allocator: Allocator,
        items: []Item,
        index: usize,
        hn: *Self,
        err: *?anyerror,
    };

    var i: usize = 0;
    var spawn_err: ?anyerror = null;
    for (ids) |id| {
        if (spawn_err != null)
            break;

        const spawn_args = try allocator.create(SpawnArgs);
        spawn_args.* = SpawnArgs{
            .id = id,
            .allocator = allocator,
            .items = items,
            .index = i,
            .hn = self,
            .err = &spawn_err,
        };

        self.thread_pool.spawnWg(
            &wg,
            struct {
                fn _(args: *SpawnArgs) void {
                    defer args.allocator.destroy(args);

                    const item = args.hn.fetchItem(args.allocator, args.id) catch |err| {
                        args.err.* = err;
                        return;
                    };

                    args.items[args.index] = item;
                }
            }._,
            .{spawn_args},
        );

        i += 1;
    }

    wg.wait();

    if (spawn_err) |err| {
        return err;
    }

    return items;
}

const AlgoliaItem = struct {
    id: ItemID = 0,
    parent_id: ?ItemID = null,
    points: ?usize = null,
    author: []const u8 = "",
    created_at_i: u64 = 0,
    type: []const u8 = "",
    title: ?[]const u8 = null,
    text: ?[]const u8 = null,
    url: ?[]const u8 = null,
    story_id: ?ItemID = null,

    sibling_num: usize = 0,

    children: []const AlgoliaItem = &.{},
};

// Caller must call Item.free() for each item and the free the slice itself
/// Uses the official Algolia API. This should be preferred since it only
/// makes one HTTP request.
pub fn fetchThreadAlgolia(self: *Self, allocator: Allocator, opID: ItemID) ![]Item {
    std.log.debug("fetching thread {d} from algolia", .{opID});
    errdefer std.log.debug("failed to fetch thread {d} from algolia", .{opID});

    const url = try std.fmt.allocPrint(
        allocator,
        "https://hn.algolia.com/api/v1/items/{d}",
        .{opID},
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
        ) !void {
            var kids = try gpa.alloc(ItemID, aitem.children.len);
            errdefer gpa.free(kids);

            var item = try (Item{
                .id = aitem.id,
                .url = aitem.url,
                .parent = aitem.parent_id,
                .score = aitem.points orelse 0,
                .by = aitem.author,
                .time = aitem.created_at_i,
                .type = aitem.type,
                .title = aitem.title orelse "",
                .text = aitem.text orelse "",
                .kids = kids,
                .sibling_num = aitem.sibling_num,
                .thread_id = aitem.story_id,
            }).dupe(gpa);
            errdefer item.free(gpa);

            for (aitem.children, 0..) |child, i| {
                kids[i] = child.id;

                var sub_item = @constCast(&child);
                sub_item.sibling_num = i;
                try loop(gpa, thread_id, items, sub_item);
            }
            item.kids = kids;

            items.appendAssumeCapacity(item);
        }
    }.loop;

    const num_items = countItems(&parsed.value);
    var result: std.ArrayList(Item) = try .initCapacity(allocator, num_items);

    defer result.deinit(allocator);
    errdefer for (result.items) |item| item.free(allocator);

    try collect(allocator, opID, &result, &parsed.value);
    std.log.debug("fetched thread {d} from algolia : got {d} items ", .{ opID, result.items.len });

    return result.toOwnedSlice(allocator);
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

    const QueueEntry = struct {
        id: ItemID,
        sibling_num: usize,
    };
    var queue: std.ArrayList(QueueEntry) = .{};
    defer queue.deinit(allocator);

    try queue.append(allocator, .{
        .id = opID,
        .sibling_num = 0,
    });

    const SpawnContext = struct {
        thread_id: usize,
        entry: QueueEntry,
        allocator: Allocator,
        items: *std.ArrayList(Item),
        queue: *std.ArrayList(QueueEntry),
        hn: *Self,
        err: *?anyerror,
        queue_mu: *std.Thread.Mutex,
        items_mu: *std.Thread.Mutex,
    };

    var wg: std.Thread.WaitGroup = .{};
    var result_mu: std.Thread.Mutex = .{};
    var queue_mu: std.Thread.Mutex = .{};

    var spawn_err: ?anyerror = null;
    while (queue.items.len > 0 or !wg.isDone()) {
        if (spawn_err != null)
            break;

        const entry = blk: {
            queue_mu.lock();
            defer queue_mu.unlock();
            break :blk queue.pop();
        } orelse continue;

        const spawn_ctx = try allocator.create(SpawnContext);
        spawn_ctx.* = SpawnContext{
            .thread_id = opID,
            .entry = entry,
            .allocator = allocator,
            .items = &result,
            .queue = &queue,
            .hn = self,
            .err = &spawn_err,
            .queue_mu = &queue_mu,
            .items_mu = &result_mu,
        };

        self.thread_pool.spawnWg(&wg, struct {
            fn _(ctx: *SpawnContext) void {
                const id = ctx.entry.id;
                const sibling_num = ctx.entry.sibling_num;

                defer ctx.allocator.destroy(ctx);

                var item = ctx.hn.fetchItem(ctx.allocator, id) catch |err| {
                    ctx.err.* = err;
                    return;
                };

                item.sibling_num = sibling_num;
                item.thread_id = ctx.thread_id;

                ctx.items_mu.lock();
                ctx.items.append(ctx.allocator, item) catch |err| {
                    ctx.items_mu.unlock();
                    ctx.err.* = err;
                    item.free(ctx.allocator);
                    return;
                };
                ctx.items_mu.unlock();

                ctx.queue_mu.lock();
                defer ctx.queue_mu.unlock();
                for (item.kids, 0..) |kid, i| {
                    const next: QueueEntry = .{
                        .id = kid,
                        .sibling_num = i,
                    };
                    ctx.queue.append(ctx.allocator, next) catch |err| {
                        ctx.err.* = err;
                        return;
                    };
                }
            }
        }._, .{spawn_ctx});
    }

    wg.wait();

    if (spawn_err) |err| {
        return err;
    }

    return result.toOwnedSlice(allocator);
}
// Caller must call Item.free() for each item and the free the slice itself
// Uses the Algolia API, then falls back to official HN API if it fails.
pub fn fetchThread(self: *Self, allocator: Allocator, opID: ItemID) ![]const Item {
    std.log.debug("fetching thread {d}", .{opID});
    errdefer std.log.debug("failed to fetch thread {d}", .{opID});

    const cached_items = try self.loadThreadCache(allocator, opID);
    if (cached_items) |ci| {
        std.log.debug("fetched cached thread {d}: found {d} items", .{ opID, ci.len });
        return ci;
    }

    const items = blk: {
        break :blk self.fetchThreadAlgolia(allocator, opID) catch |err| {
            return switch (err) {
                Error.FetchError => break :blk try self.fetchThreadOfficial(allocator, opID),
                else => return err,
            };
        };
    };

    std.log.debug("fetched thread {d}: got {d} items", .{ opID, items.len });

    std.sort.block(Item, items, {}, compareItem);

    {
        const copy = try dupeItems(allocator, items);
        std.log.debug("spawning thread for cacheThread({d})", .{opID});
        self.thread_pool.spawn(struct {
            fn _(gpa: Allocator, id: ItemID, items2: []const Item) void {
                defer std.log.debug("thread for cacheThread({d}) done", .{id});
                defer freeItems(gpa, items2);
                cacheThread(gpa, id, items2) catch |err| {
                    std.log.debug("failed to cache thread: {any}", .{err});
                    printStackTrace(@errorReturnTrace());
                };
            }
        }._, .{ allocator, opID, copy }) catch unreachable;
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

fn loadSubThreadCache(
    self: *Self,
    allocator: Allocator,
    opID: ItemID,
) !?[]const Item {
    var queue: std.ArrayList(ItemID) = try .initCapacity(allocator, 64);
    defer queue.deinit(allocator);

    var result: std.ArrayList(Item) = try .initCapacity(allocator, 16);
    defer result.deinit(allocator);
    errdefer {
        for (result.items) |item| {
            item.free(allocator);
        }
    }

    try queue.append(allocator, opID);

    while (queue.pop()) |item_id| {
        const item = try self.loadItemCache(allocator, item_id) orelse {
            return null;
        };
        try result.append(allocator, item);
        try queue.appendSlice(allocator, item.kids);
    }

    return try result.toOwnedSlice(allocator);
}

fn loadThreadCache(
    self: *Self,
    allocator: Allocator,
    opID: ItemID,
) !?[]const Item {
    const basename = try std.fmt.allocPrint(allocator, "thread-{d}", .{opID});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    const maybe_ids = self.loadCacheFile(allocator, []const ItemID, filename) catch |err| {
        switch (err) {
            Error.InvalidCacheFile => {
                return self.loadSubThreadCache(allocator, opID);
            },
            else => return err,
        }
    };

    const ids = maybe_ids orelse return null;
    defer allocator.free(ids);

    return try self.fetchAllItems(allocator, ids);
}

fn loadItemCache(self: *Self, allocator: Allocator, id: ItemID) !?Item {
    const basename = try std.fmt.allocPrint(allocator, "item-{d}", .{id});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    return self.loadCacheFile(allocator, Item, filename) catch |err| {
        if (err == Error.InvalidCacheFile) return null;
        return err;
    };
}

fn loadFeedCache(self: *Self, allocator: Allocator, feed_name: []const u8) !?[]const ItemID {
    const basename = try std.fmt.allocPrint(allocator, "feed-{s}", .{feed_name});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    return self.loadCacheFile(allocator, []const ItemID, filename) catch |err| {
        if (err == Error.InvalidCacheFile) return null;
        return err;
    };
}

fn loadCacheFile(
    self: *Self,
    allocator: Allocator,
    T: type,
    filename: []const u8,
) !?T {
    const file = std.fs.cwd().openFile(filename, .{}) catch |err| {
        switch (err) {
            std.fs.File.OpenError.FileNotFound,
            std.fs.File.OpenError.BadPathName,
            => {
                return Error.InvalidCacheFile;
            },
            else => return null,
        }
    };
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var reader = file.reader(&buffer);

    const stat = try file.stat();
    if ((std.time.nanoTimestamp() - stat.mtime) > self.cache.expiration_sec * sec_to_nano) {
        return null;
    }

    const file_size = stat.size;
    const data = try allocator.alloc(u8, file_size + 1);
    defer allocator.free(data);

    data[data.len - 1] = 0;
    try reader.interface.readSliceAll(data[0..file_size]);

    return std.zon.parse.fromSlice(T, allocator, data[0..file_size :0], null, .{
        .free_on_error = true,
    }) catch null;
}

fn cacheFeed(allocator: Allocator, feed_name: []const u8, ids: []const ItemID) !void {
    try std.fs.cwd().makePath(".zlacker-cache");

    const basename = try std.fmt.allocPrint(allocator, "feed-{s}", .{feed_name});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    const file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var writer = file.writer(&buffer);
    var w = &writer.interface;

    try std.zon.stringify.serialize(ids, .{}, w);
    return w.flush();
}

fn cacheItem(allocator: Allocator, item: Item) !void {
    try std.fs.cwd().makePath(".zlacker-cache");

    const basename = try std.fmt.allocPrint(allocator, "item-{d}", .{item.id});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    const file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var writer = file.writer(&buffer);
    var w = &writer.interface;

    try std.zon.stringify.serialize(item, .{}, w);
    return w.flush();
}

fn cacheThread(allocator: Allocator, opID: ItemID, items: []const Item) !void {
    defer std.log.debug("caching thread {d} with {d} items", .{ opID, items.len });
    try std.fs.cwd().makePath(".zlacker-cache");

    const basename = try std.fmt.allocPrint(allocator, "thread-{d}", .{opID});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    var ids: std.ArrayList(ItemID) = try .initCapacity(allocator, items.len);
    defer ids.deinit(allocator);

    for (items) |item| {
        ids.appendAssumeCapacity(item.id);
        cacheItem(allocator, item) catch |err| {
            std.debug.print("failed to cache thread item: {any}\n", .{err});
        };
    }

    const file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var writer = file.writer(&buffer);
    var w = &writer.interface;

    try std.zon.stringify.serialize(ids.items, .{}, w);
    return w.flush();
}

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

test {
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

test {
    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = std.testing.allocator,
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);
    defer hn.deinit(allocator);

    const story_ids = try hn.fetchTopStoryIDs(allocator);
    defer allocator.free(story_ids);

    const items: []const Item = try hn.fetchAllItems(allocator, story_ids[0..1]);
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

    const items = try hn.fetchThread(allocator, 1727731);
    defer freeItems(allocator, items);

    var count: usize = 0;
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

// TODO: I can't write the whole thread-12831823 file
// since accessing the sub-threads will create a whole
// new different file, which will result in a large disk usage,
// it's better to write individual item files

// TODO: use sqlite since concurrent file reading/writing
// isn't good/safe
// or not, not worth the dependency?
// if the cache file is corrupted, then just refetch, no biggie

// TODO: a background task that removes stale old cache files
// to prevent disk usage build up
// hn.startDeleteStaleCache
