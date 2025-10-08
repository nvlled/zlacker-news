const std = @import("std");
const builtin = @import("builtin");
const http = std.http;
const zqlite = @import("zqlite");
const Allocator = std.mem.Allocator;

client: http.Client,
thread_pool: *std.Thread.Pool,
db: DB,

cache: struct {
    remover_task_active: bool = false,
    wg: std.Thread.WaitGroup = .{},
} = .{},

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

    return .{
        .client = client,
        .thread_pool = tpool,
        .db = db,
    };
}

pub fn deinit(self: *Self, allocator: Allocator) void {
    self.client.deinit();
    self.thread_pool.deinit();
    self.db.deinit();
    allocator.destroy(self.thread_pool);
}

pub fn waitBackgroundTasks(self: *Self) void {
    self.thread_pool.waitAndWork(&self.cache.wg);
}

// Caller must free returned string
pub fn fetch(self: *Self, arena: Allocator, url: []const u8) ![]const u8 {
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

// Caller must call Item.free() afterwards
pub fn fetchItem(self: *Self, allocator: Allocator, id: ItemID) !Item {
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
    errdefer item.free(allocator);

    std.sort.block(ItemID, @constCast(item.kids), {}, struct {
        fn _(_: void, a: ItemID, b: ItemID) bool {
            return a < b;
        }
    }._);

    return item;
}

// Caller must call Item.free() for each item and the free the slice itself
pub fn fetchAllItems(self: *Self, allocator: Allocator, ids: []const ItemID) ![]const Item {
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
                        std.debug.print("error while fetching {d}\n", .{args.id});
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

    try self.db.insertItems(items);

    return items;
}

const AlgoliaItem = struct {
    id: ItemID = 0,
    parent_id: ?ItemID = null,
    points: ?usize = null,
    author: []const u8 = "",
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
        ) !usize {
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
                .kids = &.{},
                .thread_id = aitem.story_id,
            }).dupe(gpa);
            errdefer item.free(gpa);

            var comment_count: usize = 1;
            for (aitem.children, 0..) |child, i| {
                kids[i] = child.id;

                const sub_item = @constCast(&child);
                comment_count += try loop(gpa, thread_id, items, sub_item);
            }

            item.kids = kids;
            item.descendants = comment_count;

            items.appendAssumeCapacity(item);

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
    };
    var queue: std.ArrayList(QueueEntry) = .{};
    defer queue.deinit(allocator);

    try queue.append(allocator, .{
        .id = opID,
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

                defer ctx.allocator.destroy(ctx);

                var item = ctx.hn.fetchItem(ctx.allocator, id) catch |err| {
                    ctx.err.* = err;
                    return;
                };

                item.thread_id = ctx.thread_id;

                {
                    ctx.items_mu.lock();
                    defer ctx.items_mu.unlock();
                    ctx.items.append(ctx.allocator, item) catch |err| {
                        ctx.err.* = err;
                        item.free(ctx.allocator);
                        return;
                    };
                }

                {
                    ctx.queue_mu.lock();
                    defer ctx.queue_mu.unlock();
                    for (item.kids) |kid| {
                        const next: QueueEntry = .{
                            .id = kid,
                        };
                        ctx.queue.append(ctx.allocator, next) catch |err| {
                            ctx.err.* = err;
                            return;
                        };
                    }
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

    std.log.debug("fetched thread {d}: got {d} items", .{ opID, items.len });
    std.sort.block(Item, @constCast(items), {}, compareItem);
    try self.db.insertItems(items);

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
        opID: ItemID,
    ) !?[]const Item {
        var queue: std.ArrayList(ItemID) = .{};
        defer queue.deinit(allocator);

        var result: std.ArrayList(Item) = .{};
        errdefer {
            for (result.items) |item| item.free(allocator);
            result.deinit(allocator);
        }

        const conn = self.pool.acquire();
        defer conn.release();

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        const item_stmt = try conn.prepare(
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
        );
        defer item_stmt.deinit();

        const replies_stmt = try conn.prepare(
            \\SELECT reply_id FROM hn_item_replies
            \\WHERE item_id = ?;
        );
        defer replies_stmt.deinit();

        try queue.append(allocator, opID);

        const now = std.time.timestamp();

        while (queue.pop()) |item_id| {
            try item_stmt.reset();
            try item_stmt.bind(.{item_id});

            if (!try item_stmt.step()) {
                continue;
            }

            var item = Item{
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
            };
            const num_replies: usize = @intCast(item_stmt.int(14));

            if (now - item.inserted >= expiration_sec) {
                std.log.debug("thread cached expired: {d}", .{opID});
                for (result.items) |x| x.free(allocator);
                result.deinit(allocator);
                return null;
            }

            item = try item.dupe(allocator);
            errdefer item.free(allocator);

            if (num_replies > 0) {
                const replies: []ItemID = try allocator.alloc(ItemID, num_replies);
                errdefer allocator.free(replies);

                try replies_stmt.reset();
                try replies_stmt.bind(.{item.id});

                var i: usize = 0;
                while (try replies_stmt.step()) {
                    replies[i] = @intCast(replies_stmt.int(0));
                    i += 1;
                }
                item.kids = replies;
            }

            try result.append(allocator, item);

            try queue.ensureTotalCapacity(allocator, queue.items.len + item.kids.len);
            const kids = item.kids;
            for (0..kids.len) |i| {
                queue.appendAssumeCapacity(kids[kids.len - i - 1]);
            }
        }

        if (result.items.len == 0 or result.items.len < result.items[0].kids.len + 1) {
            return null;
        }

        const items = try result.toOwnedSlice(allocator);

        std.sort.block(Item, @constCast(items), {}, compareItem);

        return items;
    }

    pub fn readItems(
        self: @This(),
        allocator: Allocator,
        ids: []const ItemID,
    ) !?[]const Item {
        if (ids.len == 0) return &.{};

        var queue: std.ArrayList(ItemID) = try .initCapacity(allocator, ids.len);
        defer queue.deinit(allocator);

        var result: std.ArrayList(Item) = .{};
        errdefer {
            for (result.items) |item| item.free(allocator);
            result.deinit(allocator);
        }

        const conn = self.pool.acquire();
        defer conn.release();

        errdefer std.log.err("failed to read item: {s}", .{conn.lastError()});

        const item_stmt = try conn.prepare(
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
        );
        defer item_stmt.deinit();

        const replies_stmt = try conn.prepare(
            \\SELECT reply_id FROM hn_item_replies
            \\WHERE item_id = ?;
        );
        defer replies_stmt.deinit();

        for (0..ids.len) |i| {
            queue.appendAssumeCapacity(ids[ids.len - i - 1]);
        }

        var invalidated = false;
        const now = std.time.timestamp();

        for (0..ids.len) |idx| {
            const item_id = ids[idx];

            try item_stmt.reset();
            try item_stmt.bind(.{item_id});

            if (!try item_stmt.step()) {
                invalidated = true;
                break;
            }

            var item = Item{
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
            };
            const num_replies: usize = @intCast(item_stmt.int(14));

            if (now - item.inserted >= expiration_sec) {
                invalidated = true;
                break;
            }

            item = try item.dupe(allocator);
            errdefer item.free(allocator);

            if (num_replies > 0) {
                const replies: []ItemID = try allocator.alloc(ItemID, num_replies);
                errdefer allocator.free(replies);

                try replies_stmt.reset();
                try replies_stmt.bind(.{item.id});

                var i: usize = 0;
                while (try replies_stmt.step()) {
                    replies[i] = @intCast(replies_stmt.int(0));
                    i += 1;
                }
                item.kids = replies;
            }

            try result.append(allocator, item);
        }

        if (invalidated) {
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
