const std = @import("std");
const httpz = @import("httpz");

const assets = @import("./assets.zig");
const Ext2mime = @import("./ext2mime.zig");
const HN = @import("./hn.zig");
const Zhtml = @import("zhtml");
const RequestContext = @import("./context.zig");

const Action = *const fn (*RequestContext) anyerror!void;

const PageEntry = struct {
    path: []const u8,
    action: Action,
};

const pages: []const PageEntry = &.{
    .{
        .path = "/",
        .action = handleIndex,
    },
};

const App = struct {
    allocator: std.mem.Allocator,

    pub fn dispatch(app: *const App, action: Action, req: *httpz.Request, res: *httpz.Response) !void {
        var zhtml: Zhtml = if (@import("builtin").mode == .Debug)
            try .initDebug(res.writer(), res.arena)
        else
            .init(res.writer());

        // I should probably wrap the top-level allocator with ThreadSafeAllocator?
        var hn: HN = try .init(app.allocator);

        var ctx = RequestContext{
            .hn = &hn,
            .zhtml = &zhtml,
            .req = req,
            .res = res,
            .allocator = app.allocator,
        };

        return action(&ctx) catch |err| {
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
        };
    }

    pub fn uncaughtError(_: *App, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        std.log.info("500 {} {s} {}", .{ req.method, req.url.path, err });
        res.status = 500;
        res.body = "nope";
    }
};

pub fn startServer() !void {
    const allocator = std.heap.smp_allocator;

    const port = 8000;
    var server = try httpz.Server(App).init(allocator, .{ .port = port }, .{
        .allocator = allocator,
    });
    defer {
        server.stop();
        server.deinit();
    }

    var router = try server.router(.{});
    for (pages) |page| {
        router.get(page.path, page.action, .{});
    }

    router.get("/", handleIndex, .{});
    router.get("/assets/*", handleAsset, .{});

    std.debug.print("server running at localhost:{d}\n", .{port});
    try server.listen();
}

fn handleAsset(ctx: *RequestContext) !void {
    const basename = "/assets/";
    const filename = ctx.req.url.path[basename.len..];
    std.debug.print("serving asset: {s}\n", .{filename});

    const t: assets.Names = std.meta.stringToEnum(assets.Names, filename) orelse {
        return error.AssetNotFound;
    };

    const data = assets.getData(t);
    const file_ext = std.fs.path.extension(filename);

    if (Ext2mime.get(file_ext)) |mime_type| {
        ctx.res.header("Content-Type", mime_type);
    }

    ctx.res.body = data;
}

fn handleIndex(ctx: *RequestContext) !void {
    const query = try ctx.req.query();
    const page_num = if (query.get("page")) |p| std.fmt.parseInt(u8, p, 10) catch 0 else 0;

    const allocator = ctx.allocator;

    const all_ids = try ctx.hn.fetchTopStoryIDs(allocator);
    defer allocator.free(all_ids);

    const page_size: usize = 30;
    const index = page_num * page_size;
    const selected_ids = all_ids[index..@min(index + page_size, all_ids.len)];
    const items = try ctx.hn.fetchAllItems(allocator, selected_ids);
    defer HN.freeItems(allocator, items);

    // while fetching the comments I could start rendering the the
    // page too, then once I need data, I just join the thread pool
    // maybe that's not a good idea though
    // since it could show a half-broke page state in a long while
    // nothing a loading spinny indicator though
    ctx.res.header("Content-Type", "text/html");
    try @import("./pages/index.zig").render(ctx, .{ .items = items });
}

test {
    std.testing.refAllDecls(HN);
}

//test {
//    const allocator = std.testing.allocator;
//
//    var wt = httpz.testing.init(.{});
//    defer wt.deinit();
//
//    var hn: HN = try .init(allocator);
//    defer hn.deinit(allocator);
//
//    var zhtml: Zhtml = try .initDebug(wt.res.writer(), allocator);
//    defer zhtml.deinit(allocator);
//
//    for (pages) |page| {
//        var ctx: RequestContext = .{
//            .hn = &hn,
//            .zhtml = &zhtml,
//            .req = wt.req,
//            .res = wt.res,
//            .allocator = allocator,
//        };
//        try page.action(&ctx);
//        try wt.expectStatus(200);
//        std.debug.print("output for \"{s}\":\n{s}\n\n", .{ page.path, try wt.getBody() });
//    }
//}
