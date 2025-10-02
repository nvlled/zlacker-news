const std = @import("std");
const HN = @import("../hn.zig");
const Zhtml = @import("zhtml");
const Layout = @import("./layout.zig");
const RequestContext = @import("../context.zig");

pub const Data = struct {
    items: []const HN.Item,
    page_num: usize,
    page_size: usize,
};

pub fn render(ctx: *RequestContext, data: Data) !void {
    const z = ctx.zhtml;
    const layout: Layout = .{ .ctx = ctx };
    const arena = ctx.res.arena;

    layout.begin(.{});
    {
        z.div.attr(.id, "feed-page");
        z.div.@"<>"();
        for (data.items, 0..) |item, i| {
            z.div.@"<>"();
            z.h2.@"<>"();
            {
                const href = try std.fmt.allocPrint(arena, "/item?id={d}", .{item.id});
                z.a.attr(.class, "item-link");
                z.a.attr(.href, href);
                try z.a.renderf(arena, "{d}. {s}", .{
                    data.page_num * data.page_size + i + 1,
                    item.title,
                });
            }
            z.h2.@"</>"();
            z.div.@"</>"();
        }

        z.br.render();
        try z.a.attrf(arena, .href, "?page={d}", .{data.page_num + 1});
        z.a.render("[more]");
        z.div.@"</>"();
    }
    layout.end();
}
