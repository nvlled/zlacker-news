const std = @import("std");
const HN = @import("../hn.zig");
const Zhtml = @import("zhtml");
const Layout = @import("./layout.zig");
const RequestContext = @import("../context.zig");

pub const Data = struct {
    items: []const HN.Item,
};

pub fn render(ctx: *RequestContext, data: Data) !void {
    const z = ctx.zhtml;
    const layout: Layout = .{ .ctx = ctx };

    try layout.begin_();
    {
        for (data.items) |item| {
            try z.div.begin_();
            try z.h2.render_(item.title);
            try z.br.render_();
            try z.div.end();
        }
    }
    try layout.end();
}
