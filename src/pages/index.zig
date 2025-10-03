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

    try layout.begin(.{});
    {
        z.ol.attr(.id, "feed-page");
        try z.ol.attrf(arena, .start, "{d}", .{
            data.page_num * data.page_size + 1,
        });
        z.ol.@"<>"();
        for (data.items) |item| {
            z.li.@"<>"();
            {
                const href = try std.fmt.allocPrint(arena, "/item?id={d}", .{item.id});
                z.a.attr(.class, "item-link");
                z.a.attr(.href, href);
                try z.a.renderf(arena, "{s}", .{
                    item.title,
                });
                try z.div.renderf(arena, "{d} points {d} comments", .{
                    item.score, item.descendants,
                });
            }
            z.li.@"</>"();
        }
        z.ol.@"</>"();

        z.br.render();
        try z.a.attrf(arena, .href, "?page={d}", .{data.page_num + 1});
        z.a.render("[more]");
    }
    layout.end();
}

// TODO: wrap lines of <pre><code>
// TODO: add [source] links
// TODO: add [comment chain] /discussion?id=1231231
