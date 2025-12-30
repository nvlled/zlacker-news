const std = @import("std");
const httpz = @import("httpz");

const Assets = @import("./assets.zig");
const Ext2mime = @import("./ext2mime.zig");
const HN = @import("./hn.zig");
const Zhtml = @import("zhtml");
const RequestContext = @import("./context.zig");

const Action = *const fn (*RequestContext) anyerror!void;
const Render = *const fn (*Zhtml) anyerror!void;

const routes = .{
    .@"/" = @import("./pages/index.zig").serve,
    .@"/item" = @import("./pages/item.zig").serveItem,
    .@"/item/discussion" = @import("./pages/item.zig").serveDiscussion,
};

const RoutePath = std.meta.FieldEnum(@TypeOf(routes));

const Handler = struct {
    hn: *HN,
    allocator: std.mem.Allocator,
    thread_pool: ?*std.Thread.Pool = null,

    page404: []const u8 = "Not found",
    page500: []const u8 = "Internal server error",

    pub fn handle(app: Handler, req: *httpz.Request, res: *httpz.Response) void {
        handleRoute(app, req, res) catch |err| {
            switch (err) {
                error.PageNotFound => handleNotFound(app, req, res) catch {},
                else => handleInternalError(app, err, req, res),
            }
        };
    }

    pub fn handleRoute(app: Handler, req: *httpz.Request, res: *httpz.Response) !void {
        if (std.meta.stringToEnum(RoutePath, req.url.path)) |route_path| {
            try handlePage(app, route_path, req, res);
            if (@import("builtin").mode == .Debug) {
                try PageReloader.injectReloadScript(res);
            }

            return;
        }

        if (std.mem.startsWith(u8, req.url.path, "/assets/")) {
            return handleAsset(req, res);
        }

        if (@import("builtin").mode == .Debug) {
            if (std.mem.eql(u8, req.url.path, PageReloader.route_path)) {
                try PageReloader.serveWatchStream(req, res);
            }
        }

        try handleNotFound(app, req, res);
    }

    fn handleInternalError(
        app: Handler,
        err: anyerror,
        req: *httpz.Request,
        res: *httpz.Response,
    ) void {
        res.status = 500;
        res.header("Content-Type", "text/html");
        res.writer().writeAll(app.page500) catch {};

        if (@import("builtin").mode == .Debug) {
            PageReloader.injectReloadScript(res) catch {};
        }

        if (@errorReturnTrace()) |trace| std.debug.dumpStackTrace(trace.*);
        std.log.err("an error occured while serving page {s}: {any}", .{
            req.url.path,
            err,
        });
    }

    fn handleNotFound(
        app: Handler,
        _: *httpz.Request,
        res: *httpz.Response,
    ) !void {
        res.status = 404;
        res.header("Content-Type", "text/html");
        res.writer().writeAll(app.page404) catch {};

        if (@import("builtin").mode == .Debug) {
            try PageReloader.injectReloadScript(res);
        }
    }

    fn handlePage(
        app: Handler,
        route_path: RoutePath,
        req: *httpz.Request,
        res: *httpz.Response,
    ) !void {
        var zhtml: Zhtml = try .init(res.writer(), res.arena);
        defer {
            if (zhtml.getErrorTrace()) |trace| std.debug.print("{s}\n", .{trace});
            zhtml.deinit(res.arena);
        }
        defer res.writer().flush() catch {};

        var ctx = RequestContext{
            .zhtml = &zhtml,
            .hn = app.hn,
            .req = req,
            .res = res,
            .allocator = app.allocator,
            .thread_pool = app.thread_pool,
        };

        const handler = getRouteHandler(route_path);

        ctx.res.header("Content-Type", "text/html");
        handler(&ctx) catch |err| {
            return err;
        };

        try zhtml.getError();
    }

    pub fn uncaughtError(_: *Handler, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        std.log.info("500 {} {s} {}", .{ req.method, req.url.path, err });
        res.status = 500;
        res.body = "nope";
    }

    fn handleAsset(req: *httpz.Request, res: *httpz.Response) !void {
        const path = req.url.path;
        const filename = path[0..@min(path.len, Assets.max_name_len)];

        const t: Assets.Names = std.meta.stringToEnum(Assets.Names, filename) orelse {
            res.status = 404;
            return error.AssetNotFound;
        };

        const data = Assets.getData(t);
        const file_ext = std.fs.path.extension(filename);

        if (Ext2mime.get(file_ext)) |mime_type| {
            res.header("Content-Type", mime_type);
        }

        res.body = data;
    }

    fn getRouteHandler(route_path: RoutePath) Action {
        return switch (route_path) {
            inline else => |x| @field(routes, @tagName(x)),
        };
    }
};

fn renderToString(allocator: std.mem.Allocator, render: Render) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();

    var zhtml: Zhtml = try .init(&buf.writer, allocator);
    defer {
        if (zhtml.getErrorTrace()) |trace| std.debug.print("{s}\n", .{trace});
        zhtml.deinit(allocator);
    }

    try render(&zhtml);

    return buf.toOwnedSlice();
}

const PageReloader = struct {
    const reload_script =
        \\
        \\<script>
        \\const source = new EventSource("/.watch");
        \\window.onbeforeunload = () => source.close();
        \\source.onerror = async () => {
        \\  source.close();
        \\  let maxRetries = 1000;
        \\  let okays = 0;
        \\  while (maxRetries-- > 0) {
        \\    try {
        \\      if ((await fetch("/")).status === 200) {
        \\        okays++;
        \\        if (okays > 10) {
        \\          location.reload();
        \\          break;
        \\        }
        \\      }
        \\    } catch (e) { okays = 0; }
        \\  }
        \\}
        \\</script>
    ;

    const route_path = "/.watch";

    pub fn serveWatchStream(_: *httpz.Request, res: *httpz.Response) !void {
        try res.startEventStream({}, struct {
            fn _(_: void, stream: std.net.Stream) void {
                while (true) {
                    stream.writeAll("event: ping\ndata:\n\n") catch return;
                    std.Thread.sleep(1e6 * 1000);
                }
            }
        }._);
    }

    pub fn injectReloadScript(res: *httpz.Response) !void {
        return res.writer().writeAll(reload_script);
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

    const page404 = try renderToString(allocator, @import("pages/error404.zig").render);
    defer allocator.free(page404);

    const page500 = try renderToString(allocator, @import("pages/error500.zig").render);
    defer allocator.free(page500);

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
        .page404 = page404,
        .page500 = page500,
    });
    defer {
        server.stop();
        server.deinit();
    }

    std.debug.print("server running at localhost:{d}\n", .{port});
    try server.listen();

    unreachable;
}

const test_output = @import("build_options").test_output;

test "routes:ndex" {
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
    try @import("./pages/index.zig").serve(&ctx);
    try wt.expectStatus(200);
    if (test_output)
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
    try routes.@"/item"(&ctx);
    try wt.expectStatus(200);
    if (test_output)
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

    ctx.req.url = .parse("/assets/style.css");

    try Handler.handleAsset(ctx.req, ctx.res);
    try wt.expectStatus(200);
    if (test_output)
        std.debug.print("test output for \"{s}\":\n{s}\n\n", .{
            "/assets/style.css",
            try wt.getBody(),
        });
}

test {
    std.testing.refAllDeclsRecursive(@This());
}
