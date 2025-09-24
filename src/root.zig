const std = @import("std");
const httpz = @import("httpz");
const Zhtml = @import("zhtml");
const RequestContext = @import("./context.zig");
const HN = @import("./hn.zig");

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

    pub fn dispatch(self: *const App, action: Action, req: *httpz.Request, res: *httpz.Response) !void {
        _ = self;
        var zhtml: Zhtml = if (@import("builtin").mode == .Debug)
            try .initDebug(res.writer(), res.arena)
        else
            .init(res.writer());

        var ctx = RequestContext{
            .zhtml = &zhtml,
            .req = req,
            .res = res,
        };

        return action(&ctx) catch |err| {
            if (@errorReturnTrace()) |trace| {
                const w = res.writer();
                try w.writeAll("<br><pre style='color: red; background: #222'><h1>Error</h1>");
                try std.debug.writeStackTrace(trace.*, res.writer(), try std.debug.getSelfDebugInfo(), .no_color);
                try w.writeAll("<pre>");
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

    std.debug.print("server running at localhost:{d}\n", .{port});
    try server.listen();
}

fn handleIndex(ctx: *RequestContext) !void {
    // while fetching the comments I could start rendering the the
    // page too, then once I need data, I just join the thread pool
    // maybe that's not a good idea though
    // since it could show a half-broke page state in a long while
    // nothing a loading spinny indicator though
    try @import("./pages/index.zig").render(ctx, .{ .x = 123 });
}

test {
    std.testing.refAllDecls(HN);
}

test {
    // but only the config.request and config.response settings have any impact
    var wt = httpz.testing.init(.{});
    defer wt.deinit();

    for (pages) |page| {
        var zhtml: Zhtml = try .initDebug(wt.res.writer(), wt.res.arena);

        var ctx: RequestContext = .{
            .zhtml = &zhtml,
            .req = wt.req,
            .res = wt.res,
        };
        try page.action(&ctx);
        try wt.expectStatus(200);
        std.debug.print("output for \"{s}\":\n{s}\n\n", .{ page.path, try wt.getBody() });
    }

    //try wt.expectJson(.{ .@"error" = "missing parameter", .parameter = "search" });
}
