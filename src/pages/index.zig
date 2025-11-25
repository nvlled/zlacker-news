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

    try layout.begin(.{});
    {
        z.ol.attr(.id, "feed-page");
        try z.ol.attrf(.start, "{d}", .{
            data.page_num * data.page_size + 1,
        });
        z.ol.@"<>"();
        for (data.items) |item| {
            z.li.@"<>"();
            {
                z.a.attr(.class, "item-link");
                try z.a.attrf(.href, "/item?id={d}", .{item.id});
                try z.a.renderf("{s}", .{item.title});
                try z.div.renderf("{d} points {d} comments", .{
                    item.score, item.descendants,
                });
            }
            z.li.@"</>"();
        }
        z.ol.@"</>"();

        z.br.render();
        try z.a.attrf(.href, "?page={d}", .{data.page_num + 1});
        z.a.render("[more]");
    }
    layout.end();
}

// TODO: add [source] links
// TODO: add [comment chain] /discussion?id=1231231
