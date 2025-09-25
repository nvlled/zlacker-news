const std = @import("std");
const builtin = @import("builtin");
const http = std.http;
const Allocator = std.mem.Allocator;

client: http.Client,
thread_pool: *std.Thread.Pool,

cache_duration_sec: u64 = if (builtin.mode == .Debug) 30 * 60 else 1 * 60,
background_wg: std.Thread.WaitGroup = .{},

pub const Error = error{FetchError};

const Self = @This();

const ItemID = u64;

const String = []const u8;

pub const Item = struct {
    by: String = "",
    id: ItemID = 0,
    score: ItemID = 0,
    title: String = "",
    url: String = "",
    descendants: usize = 0,
    kids: []const ItemID = &.{},
    time: ItemID = 0,
    type: String = "",
    text: String = "",
    deleted: bool = false,
    dead: bool = false,
    parent: ItemID = 0,

    // Caller must free Item.free() afterwards
    pub fn dupe(self: Item, allocator: Allocator) !Item {
        const by = try allocator.dupe(u8, self.by);
        errdefer allocator.free(by);

        const title = try allocator.dupe(u8, self.title);
        errdefer allocator.free(title);

        const url = try allocator.dupe(u8, self.url);
        errdefer allocator.free(url);

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
        };
    }

    pub fn free(self: Item, allocator: Allocator) void {
        allocator.free(self.by);
        allocator.free(self.title);
        allocator.free(self.url);
        allocator.free(self.kids);
        allocator.free(self.type);
        allocator.free(self.text);
        //allocator.destroy(self);
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
    self.thread_pool.waitAndWork(&self.background_wg);
}

// Caller must free returned string
pub fn fetch(self: *Self, arena: Allocator, url: []const u8) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(arena);
    defer buf.deinit();

    const result = try self.client.fetch(.{
        .method = .GET,
        .location = .{ .url = url },
        .response_writer = &buf.writer,
    });

    if (result.status != .ok) return Error.FetchError;

    return buf.toOwnedSlice();
}

// Caller must free returned slice
pub fn fetchTopStoryIDs(self: *Self, allocator: Allocator) ![]const ItemID {
    const json_str = try self.fetch(allocator, "https://hacker-news.firebaseio.com/v0/topstories.json");
    defer allocator.free(json_str);

    const parsed = try std.json.parseFromSlice([]const u64, allocator, json_str, .{});
    defer parsed.deinit();

    return allocator.dupe(ItemID, parsed.value);
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

    return parsed.value.dupe(allocator);
}

// Caller must call Item.free() for each item and the free the slice itself
pub fn fetchAllItems(self: *Self, allocator: Allocator, ids: []const ItemID) ![]Item {
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

    children: []const AlgoliaItem = &.{},
};

// Caller must call Item.free() for each item and the free the slice itself
/// Uses the official Algolia API. This should be preferred since it only
/// makes one HTTP request.
pub fn fetchThreadAlgolia(self: *Self, allocator: Allocator, opID: ItemID) ![]Item {
    const url = try std.fmt.allocPrint(
        allocator,
        "https://hn.algolia.com/api/v1/items/{d}",
        .{opID},
    );
    defer allocator.free(url);

    const json_str = try self.fetch(allocator, url);
    defer allocator.free(json_str);

    const parsed = try std.json.parseFromSlice(AlgoliaItem, allocator, json_str, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const countItems = struct {
        fn loop(aitem: *const AlgoliaItem) usize {
            var count: usize = 1;
            for (aitem.children) |child| count += loop(&child);
            return count;
        }
    }.loop;

    const collect = struct {
        fn loop(gpa: Allocator, items: *std.ArrayList(Item), aitem: *const AlgoliaItem) !void {
            const item = try (Item{
                .id = aitem.id,
                .parent = aitem.parent_id orelse 0,
                .score = aitem.points orelse 0,
                .by = aitem.author,
                .time = aitem.created_at_i,
                .type = aitem.type,
                .title = aitem.title orelse "",
                .text = aitem.text orelse "",
                .kids = &.{},
            }).dupe(gpa);
            errdefer item.free(gpa);

            items.appendAssumeCapacity(item);

            for (aitem.children) |child| {
                try loop(gpa, items, &child);
            }
        }
    }.loop;

    const num_items = countItems(&parsed.value);
    var result: std.ArrayList(Item) = try .initCapacity(allocator, num_items);

    defer result.deinit(allocator);
    errdefer for (result.items) |item| item.free(allocator);

    try collect(allocator, &result, &parsed.value);

    return result.toOwnedSlice(allocator);
}

/// Caller must call Item.free() for each item and the free the slice itself
/// Uses the official HN API. About at least two times slower compared
/// to fetchThreadAlgolia() since this once makes recursive HTTP requests
/// for each item.
pub fn fetchThreadOfficial(self: *Self, allocator: Allocator, opID: ItemID) ![]Item {
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

    const SpawnContext = struct {
        id: ItemID,
        allocator: Allocator,
        items: *std.ArrayList(Item),
        queue: *std.ArrayList(ItemID),
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

        const id = blk: {
            queue_mu.lock();
            defer queue_mu.unlock();
            break :blk queue.pop();
        } orelse continue;

        const spawn_ctx = try allocator.create(SpawnContext);
        spawn_ctx.* = SpawnContext{
            .id = id,
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
                defer ctx.allocator.destroy(ctx);

                const item = ctx.hn.fetchItem(ctx.allocator, ctx.id) catch |err| {
                    ctx.err.* = err;
                    return;
                };

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
                for (item.kids) |kid| {
                    ctx.queue.append(ctx.allocator, kid) catch |err| {
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
    const cached_items = try loadThreadCache(allocator, opID);
    if (cached_items) |ci| return ci;

    const items = blk: {
        break :blk self.fetchThreadAlgolia(allocator, opID) catch |err| {
            return switch (err) {
                Error.FetchError => break :blk try self.fetchThreadOfficial(allocator, opID),
                else => return err,
            };
        };
    };

    self.thread_pool.spawnWg(&self.background_wg, cacheThread, .{ allocator, opID, items });

    return items;
}

// Frees both slice and the individual items.
fn freeItems(allocator: Allocator, items: []const Item) void {
    for (items) |item| item.free(allocator);
    allocator.free(items);
}

//fn loadCache(allocator: Allocator, comptime T: type, filename: []const u8) !T {}

fn loadThreadCache(allocator: Allocator, opID: ItemID) !?[]const Item {
    const basename = try std.fmt.allocPrint(allocator, "thread-{d}", .{opID});
    defer allocator.free(basename);

    const filename = try std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    });
    defer allocator.free(filename);

    const file = std.fs.cwd().openFile(filename, .{}) catch |err| {
        switch (err) {
            std.fs.File.OpenError.FileNotFound,
            std.fs.File.OpenError.BadPathName,
            => {
                return null;
            },
            else => return null,
        }
    };
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var reader = file.reader(&buffer);

    const file_size = (try file.stat()).size;
    const data = try allocator.alloc(u8, file_size + 1);
    defer allocator.free(data);

    data[data.len - 1] = 0;
    try reader.interface.readSliceAll(data[0..file_size]);

    return try std.zon.parse.fromSlice([]const Item, allocator, data[0..file_size :0], null, .{
        .free_on_error = true,
    });
}

fn cacheItem(allocator: Allocator, item: Item) void {
    @panic("TODO");
}

fn loadItemCache(allocator: Allocator, id: ItemID) !?Item {
    @panic("TODO");
}

// I could also cache the portions of rendered page,
// at the the static ones

fn cacheThread(allocator: Allocator, opID: ItemID, items: []Item) void {
    std.fs.cwd().makePath(".zlacker-cache") catch |err| {
        std.debug.print("failed to create cache dir: {any}\n", .{err});
        return;
    };

    const basename = std.fmt.allocPrint(allocator, "thread-{d}", .{opID}) catch |err| {
        std.debug.print("failed to allocate basename : {any}\n", .{err});
        return;
    };
    defer allocator.free(basename);

    const filename = std.fs.path.join(allocator, &.{
        ".zlacker-cache",
        basename,
    }) catch |err| {
        std.debug.print("failed to allocate filename : {any}\n", .{err});
        return;
    };
    defer allocator.free(filename);

    const file = std.fs.cwd().createFile(filename, .{}) catch |err| {
        std.debug.print("failed to open file: {s} : {any}\n", .{ filename, err });
        return;
    };
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var writer = file.writer(&buffer);
    var w = &writer.interface;

    std.zon.stringify.serialize(items, .{}, w) catch |err| {
        std.debug.print("failed to serialize items : {any}\n", .{err});
    };

    w.flush() catch {};
}

//test {
//    var ts_allocator: std.heap.ThreadSafeAllocator = .{
//        .child_allocator = std.testing.allocator,
//    };
//    const allocator = ts_allocator.allocator();
//
//    var hn: Self = try .init(allocator);
//    defer hn.deinit(allocator);
//
//    const item = try hn.fetchItem(allocator, 45346387);
//    defer item.free(allocator);
//
//    std.debug.print("> {s} by {s}\n", .{ item.title, item.by });
//}
//
//test {
//    var ts_allocator: std.heap.ThreadSafeAllocator = .{
//        .child_allocator = std.testing.allocator,
//    };
//    const allocator = ts_allocator.allocator();
//
//    var hn: Self = try .init(allocator);
//    defer hn.deinit(allocator);
//
//    const story_ids = try hn.fetchTopStoryIDs(allocator);
//    defer allocator.free(story_ids);
//
//    const items: []Item = try hn.fetchAllItems(allocator, story_ids[0..10]);
//    defer {
//        for (items) |item| item.free(allocator);
//        allocator.free(items);
//    }
//
//    for (items) |item| {
//        std.debug.print("> {s}\n", .{item.title});
//    }
//
//    std.debug.print("item size: {d}\n", .{@sizeOf(Item)});
//}

//test {
//    var ts_allocator: std.heap.ThreadSafeAllocator = .{
//        .child_allocator = std.testing.allocator,
//    };
//    const allocator = ts_allocator.allocator();
//
//    var hn: Self = try .init(allocator);
//    defer hn.deinit(allocator);
//
//    const items = try hn.fetchThread(allocator, 45345950);
//    defer {
//        for (items) |item| item.free(allocator);
//        allocator.free(items);
//    }
//
//    for (items) |item| {
//        std.debug.print("> {s} : {s}\n{s}\n--------\n", .{
//            item.title,
//            item.by,
//            item.text,
//        });
//    }
//    std.debug.print("total num items: {d}\n", .{items.len});
//}

test {
    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = std.testing.allocator,
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);
    defer hn.deinit(allocator);

    const items = try hn.fetchThread(allocator, 45366867);
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

    hn.waitBackgroundTasks();
}
