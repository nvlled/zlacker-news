const std = @import("std");
const Zhtml = @import("zhtml");
const Layout = @import("./layout.zig");
const RequestContext = @import("../context.zig");

pub const Data = struct {
    x: u8,
};

pub fn render(ctx: *RequestContext, data: Data) !void {
    const arena = ctx.res.arena;
    const z = ctx.zhtml;
    const layout: Layout = .{ .ctx = ctx };

    try layout.begin_();
    {
        try z.p.begin_();
        try z.print(arena, "x = {d}", .{data.x});
        try z.p.end();
    }
    try layout.end();
}
