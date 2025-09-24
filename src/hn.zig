const std = @import("std");
const builtin = @import("builtin");
const http = std.http;
const Allocator = std.mem.Allocator;

client: http.Client,
thread_pool: *std.Thread.Pool,

const Self = @This();

const ItemID = u64;

const String = []const u8;

const Item = struct {
    by: String = "",
    id: ItemID = 0,
    score: ItemID = 0,
    title: String = "",
    url: String = "",
    descendants: usize = 0,
    kids: []const ItemID = &.{},
    time: ItemID = 0,
    type: String = "",

    deleted: bool = false,
    text: String = "",
    dead: bool = false,
    parent: ItemID = 0,
    poll: String = "",
    parts: []const usize = &.{},
};

pub fn init(ts_arena: Allocator) !Self {
    const client: http.Client = .{ .allocator = ts_arena };

    const tpool: *std.Thread.Pool = try ts_arena.create(std.Thread.Pool);
    try tpool.init(.{
        .allocator = ts_arena,
        .n_jobs = 32,
    });

    return .{
        .client = client,
        .thread_pool = tpool,
    };
}

pub fn deinit(self: *Self) void {
    self.client.deinit();
    self.thread_pool.deinit();
}

pub fn fetch(self: *Self, arena: Allocator, url: []const u8) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(arena);
    defer buf.deinit();

    const result = try self.client.fetch(.{
        .method = .GET,
        .location = .{ .url = url },
        .response_writer = &buf.writer,
    });

    if (result.status != .ok) return error.Huh;

    const contents = try buf.toOwnedSlice();

    return contents;
}

pub fn fetchTopStoryIDs(self: *Self, arena: Allocator) ![]const ItemID {
    const json_str = try self.fetch(arena, "https://hacker-news.firebaseio.com/v0/topstories.json");
    defer arena.free(json_str);

    const result = try std.json.parseFromSlice([]const u64, arena, json_str, .{});
    return result.value;
}

pub fn fetchItem(self: *Self, arena: Allocator, id: ItemID) !Item {
    const url = try std.fmt.allocPrint(arena, "https://hacker-news.firebaseio.com/v0/item/{d}.json", .{
        id,
    });
    defer arena.free(url);

    const json_str = try self.fetch(arena, url);

    return try std.json.parseFromSliceLeaky(Item, arena, json_str, .{
        .ignore_unknown_fields = true,
    });
}

pub fn fetchAllItems(self: *Self, arena: Allocator, ids: []const ItemID) ![]Item {
    const items: []Item = try arena.alloc(Item, ids.len);

    var wg: std.Thread.WaitGroup = .{};

    const SpawnArgs = struct {
        id: ItemID,
        allocator: Allocator,
        items: []Item,
        index: usize,
        hn: *Self,
        err: ?anyerror,
    };

    var i: usize = 0;
    for (ids) |id| {
        const spawn_args = try arena.create(SpawnArgs);
        spawn_args.* = SpawnArgs{
            .id = id,
            .allocator = arena,
            .items = items,
            .index = i,
            .hn = self,
            .err = null,
        };
        self.thread_pool.spawnWg(
            &wg,
            struct {
                fn _(args: *SpawnArgs) void {
                    const item = args.hn.fetchItem(args.allocator, args.id) catch |err| {
                        args.err = err;
                        return;
                    };

                    args.items[args.index] = item;
                }
            }._,
            .{spawn_args},
        );

        if (spawn_args.err) |err| {
            return err;
        }

        i += 1;
    }

    wg.wait();

    return items;
}

//pub fn fetchThread(self: *Self, arena: Allocator, id: ItemID) ![]const Item {
//    @panic("TODO");
//}

test {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    var ts_allocator: std.heap.ThreadSafeAllocator = .{
        .child_allocator = arena.allocator(),
    };
    const allocator = ts_allocator.allocator();

    var hn: Self = try .init(allocator);

    const story_ids = try hn.fetchTopStoryIDs(allocator);

    const items: []Item = try hn.fetchAllItems(allocator, story_ids[0..10]);
    defer allocator.free(items);

    for (items) |item| {
        std.debug.print("> {s}\n", .{item.title});
    }
}
