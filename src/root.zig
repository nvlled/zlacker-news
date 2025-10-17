const std = @import("std");
const httpz = @import("httpz");

const Assets = @import("./assets.zig");
const Ext2mime = @import("./ext2mime.zig");
const HN = @import("./hn.zig");
const Zhtml = @import("zhtml");
const RequestContext = @import("./context.zig");

const Action = *const fn (*RequestContext) anyerror!void;

const Handler = struct {
    hn: *HN,
    allocator: std.mem.Allocator,
    thread_pool: ?*std.Thread.Pool = null,

    pub fn dispatch(app: *const Handler, action: Action, req: *httpz.Request, res: *httpz.Response) !void {
        var zhtml: Zhtml = try .init(res.writer(), res.arena);
        defer {
            std.debug.print("{s}\n", .{zhtml.getErrorTrace() orelse ""});
            zhtml.deinit(res.arena);
        }

        var ctx = RequestContext{
            .hn = app.hn,
            .zhtml = &zhtml,
            .req = req,
            .res = res,
            .allocator = app.allocator,
            .thread_pool = app.thread_pool,
        };

        std.debug.print("[request] {s}\n", .{req.url.raw});

        return action(&ctx) catch |err| {
            ctx.res.status = 500;
            ctx.res.header("Content-Type", "text/html");
            if (@import("builtin").mode == .Debug) {
                if (@errorReturnTrace()) |trace| {
                    var w = res.writer();
                    try w.writeAll("<br><pre style='color: red; background: #222'><h1>Error</h1>");
                    try std.debug.writeStackTrace(trace.*, w, try std.debug.getSelfDebugInfo(), .no_color);
                    try w.writeAll("<pre>");

                    var stdout = std.fs.File.stdout().writer(&.{});
                    w = &stdout.interface;
                    try std.debug.writeStackTrace(trace.*, w, try std.debug.getSelfDebugInfo(), .escape_codes);
                }
                return err;
            } else {
                res.body = "a disturbance in the air, something went wrong";
            }
        };
    }

    pub fn uncaughtError(_: *Handler, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        std.log.info("500 {} {s} {}", .{ req.method, req.url.path, err });
        res.status = 500;
        res.body = "nope";
    }
};

pub fn startServer() !void {
    const allocator = std.heap.smp_allocator;

    var hn: HN = try .init(allocator);
    defer hn.deinit(allocator);

    try hn.db.startCleaner();

    var tpool: std.Thread.Pool = undefined;
    try tpool.init(.{
        .allocator = allocator,
    });
    defer tpool.deinit();

    const port = 8000;
    var server = try httpz.Server(Handler).init(allocator, .{
        .address = "0.0.0.0",
        .port = port,
        .timeout = .{
            .request = 1000,
        },
    }, .{
        .thread_pool = &tpool,
        .allocator = allocator,
        .hn = &hn,
    });
    defer {
        server.stop();
        server.deinit();
    }

    var router = try server.router(.{});

    router.get("/", Controllers.index, .{});
    router.get("/item", Controllers.item, .{});
    router.get("/item/discussion", Controllers.discussion, .{});
    router.get("/assets/*", Controllers.assets, .{});

    std.debug.print("server running at localhost:{d}\n", .{port});
    try server.listen();

    unreachable;
}

const Controllers = struct {
    fn assets(ctx: *RequestContext) !void {
        const path = ctx.req.url.path;
        const filename = path[0..@min(path.len, Assets.max_name_len)];

        const t: Assets.Names = std.meta.stringToEnum(Assets.Names, filename) orelse {
            ctx.res.status = 404;
            return error.AssetNotFound;
        };

        const data = Assets.getData(t);
        const file_ext = std.fs.path.extension(filename);

        if (Ext2mime.get(file_ext)) |mime_type| {
            ctx.res.header("Content-Type", mime_type);
        }

        ctx.res.body = data;
    }

    fn index(ctx: *RequestContext) !void {
        const query = try ctx.req.query();
        const page_num = if (query.get("page")) |p| std.fmt.parseInt(u8, p, 10) catch 0 else 0;
        const allocator = ctx.allocator;

        const page_size: usize = 20;
        const i = page_num * page_size;

        const ids = try ctx.hn.fetchTopStoryIDs(allocator, page_size, i);
        defer allocator.free(ids);

        var wg: std.Thread.WaitGroup = .{};
        const items = try ctx.hn.fetchAllItems(allocator, ids, .{ .cache_write = &wg });
        defer {
            wg.wait();
            HN.freeItems(allocator, items);
        }

        ctx.res.header("Content-Type", "text/html");
        try @import("./pages/index.zig").render(ctx, .{
            .items = items,
            .page_num = page_num,
            .page_size = page_size,
        });
    }

    fn item(ctx: *RequestContext) !void {
        const allocator = ctx.allocator;
        const query = try ctx.req.query();
        const id = if (query.get("id")) |p| std.fmt.parseInt(HN.ItemID, p, 10) catch {
            return error.InvalidID;
        } else {
            return error.@"Need an ID";
        };
        const with_links_only = query.get("links") != null;

        var wg: std.Thread.WaitGroup = .{};
        const items = try ctx.hn.fetchThread(allocator, id, .{ .wg = &wg });
        defer {
            wg.wait();
            HN.freeItems(allocator, items);
        }

        var lookup: std.AutoHashMapUnmanaged(HN.ItemID, HN.Item) = .{};
        defer lookup.deinit(allocator);
        try HN.fillItems(allocator, @constCast(items), &lookup);

        ctx.res.header("Content-Type", "text/html");
        try @import("./pages/item.zig").render(ctx, .{
            .items = items,
            .item_lookup = &lookup,
            .with_links_only = with_links_only,
        });
    }

    fn discussion(ctx: *RequestContext) !void {
        const allocator = ctx.allocator;
        const query = try ctx.req.query();
        const thread_id = if (query.get("tid")) |p| std.fmt.parseInt(HN.ItemID, p, 10) catch {
            return error.InvalidID;
        } else {
            return error.@"Need an tid (thread id)";
        };
        const item_id = if (query.get("id")) |p| std.fmt.parseInt(HN.ItemID, p, 10) catch {
            return error.InvalidID;
        } else {
            return error.@"Need an id (item id)";
        };

        const items = try ctx.hn.fetchDiscussion(allocator, thread_id, item_id);
        defer HN.freeItems(allocator, items);

        var lookup: std.AutoHashMapUnmanaged(HN.ItemID, HN.Item) = .{};
        defer lookup.deinit(allocator);
        try HN.fillItems(allocator, @constCast(items), &lookup);

        ctx.res.header("Content-Type", "text/html");
        try @import("./pages/item.zig").render(ctx, .{
            .items = items,
            .item_lookup = &lookup,
            .discussion_mode = true,
        });
    }
};

test "routes:index" {
    const allocator = std.testing.allocator;

    var wt = httpz.testing.init(.{});
    defer wt.deinit();

    var hn: HN = try .init(allocator);
    defer hn.deinit(allocator);

    var zhtml: Zhtml = try .init(wt.res.writer(), allocator);
    defer zhtml.deinit(allocator);

    var ctx: RequestContext = .{
        .hn = &hn,
        .zhtml = &zhtml,
        .req = wt.req,
        .res = wt.res,
        .allocator = allocator,
        .thread_pool = null,
    };
    try Controllers.index(&ctx);
    try wt.expectStatus(200);
    std.debug.print("test output for \"{s}\":\n{s}\n\n", .{ "/", try wt.getBody() });
}

test "routes:item" {
    const allocator = std.testing.allocator;
    var wt = httpz.testing.init(.{});
    defer wt.deinit();

    var hn: HN = try .init(allocator);
    defer hn.deinit(allocator);

    var zhtml: Zhtml = try .init(wt.res.writer(), allocator);
    defer zhtml.deinit(allocator);

    var ctx: RequestContext = .{
        .hn = &hn,
        .zhtml = &zhtml,
        .req = wt.req,
        .res = wt.res,
        .allocator = allocator,
        .thread_pool = null,
    };

    const q = try wt.req.query();
    q.add("id", "1727731");
    try Controllers.item(&ctx);
    try wt.expectStatus(200);
    std.debug.print("test output for \"{s}\":\n{s}\n\n", .{ "/item", try wt.getBody() });
}

test "routes:assets" {
    const allocator = std.testing.allocator;
    var wt = httpz.testing.init(.{});
    defer wt.deinit();

    var hn: HN = try .init(allocator);
    defer hn.deinit(allocator);

    var zhtml: Zhtml = try .init(wt.res.writer(), allocator);
    defer zhtml.deinit(allocator);

    var ctx: RequestContext = .{
        .hn = &hn,
        .zhtml = &zhtml,
        .req = wt.req,
        .res = wt.res,
        .allocator = allocator,
        .thread_pool = null,
    };

    ctx.req.url = .parse("/assets/script.js");

    try Controllers.assets(&ctx);
    try wt.expectStatus(200);
    std.debug.print("test output for \"{s}\":\n{s}\n\n", .{
        "/assets/script.js",
        try wt.getBody(),
    });
}

test {
    std.testing.refAllDeclsRecursive(@This());
}
