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

pub fn serve(ctx: *RequestContext) !void {
    const query = try ctx.req.query();
    const page_num = if (query.get("page")) |p| std.fmt.parseInt(u8, p, 10) catch 0 else 0;
    const allocator = ctx.allocator;

    const page_size: usize = 20;
    const i = page_num * page_size;

    const ids = try ctx.hn.fetchTopStoryIDs(allocator, page_size, i);
    defer allocator.free(ids);

    var wg: std.Thread.WaitGroup = .{};
    const items = try ctx.hn.fetchAllItems(allocator, ids, .{
        .write_cache = true,
        .write_cache_wg = &wg,
    });
    defer {
        wg.wait();
        HN.freeItems(allocator, items);
    }
    std.sort.block(
        HN.Item,
        @constCast(items),
        {},
        struct {
            fn _(_: void, a: HN.Item, b: HN.Item) bool {
                return a.time > b.time;
            }
        }._,
    );

    ctx.res.header("Content-Type", "text/html");
    try render(ctx, .{
        .items = items,
        .page_num = page_num,
        .page_size = page_size,
    });
}

pub fn render(ctx: *RequestContext, data: Data) !void {
    const z = ctx.zhtml;
    const layout: Layout = .{ .zhtml = z };

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
