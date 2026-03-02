const std = @import("std");
const HN = @import("../hn.zig");
const Zhtml = @import("zhtml");
const Layout = @import("./layout.zig");
const RequestContext = @import("../context.zig");

pub const Data = struct {};

pub fn serve(ctx: *RequestContext) !void {
    ctx.res.header("Content-Type", "text/html");
    const form_data = try ctx.req.formData();

    if (form_data.get("url")) |url| {
        if (parseItemID(url)) |id| {
            const dest = try std.fmt.allocPrint(ctx.res.arena, "/item?id={s}", .{id});

            ctx.res.setStatus(.temporary_redirect);
            ctx.res.header("Location", dest);
        }
    }

    try render(ctx, .{});
}

pub fn render(ctx: *RequestContext, _: Data) !void {
    const z = ctx.zhtml;
    const layout: Layout = .{ .zhtml = z };

    try layout.begin(.{});
    {
        z.p.render("invalid HN URL");
    }
    layout.end();
}

fn parseItemID(url: []const u8) ?[]const u8 {
    var i = std.mem.indexOfScalar(u8, url, '?') orelse return null;

    while (i < url.len) {
        const j = std.mem.indexOfScalarPos(u8, url, i + 1, '=') orelse return null;
        const k = std.mem.indexOfScalarPos(u8, url, j + 1, '&') orelse url.len;
        const key = url[i + 1 .. j];
        if (!std.mem.eql(u8, key, "id")) {
            i = k;
            continue;
        }

        const value = url[j + 1 .. k];

        for (value) |c| switch (c) {
            '0'...'9' => {},
            else => return null,
        };

        return value;
    }

    return null;
}

test parseItemID {
    try std.testing.expectEqualStrings("1234", parseItemID("foobar?id=1234") orelse "");
    try std.testing.expectEqualStrings("1234", parseItemID("foobar?x=1&id=1234") orelse "");
    try std.testing.expectEqualStrings("1234", parseItemID("foobar?x=1&id=1234&fo") orelse "");
    try std.testing.expectEqualStrings("1234", parseItemID("id=123?x=1&id=1234&fo") orelse "");
    try std.testing.expectEqualStrings("x", parseItemID("") orelse "x");
    try std.testing.expectEqualStrings("1234", parseItemID("?id=1234") orelse "");
    try std.testing.expectEqualStrings("", parseItemID("?id=abds$!@#!@") orelse "");
}
