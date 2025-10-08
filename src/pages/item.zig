const std = @import("std");
const zeit = @import("zeit");
const Allocator = std.mem.Allocator;
const sprintf = std.fmt.allocPrint;
const HN = @import("../hn.zig");
const Zhtml = @import("zhtml");
const Layout = @import("./layout.zig");
const RequestContext = @import("../context.zig");

pub const Data = struct {
    items: []const HN.Item,
    item_lookup: *std.AutoHashMapUnmanaged(HN.ItemID, HN.Item),
};

pub fn render(ctx: *RequestContext, data: Data) !void {
    const z = ctx.zhtml;
    const layout: Layout = .{ .ctx = ctx };
    const arena = ctx.res.arena;
    const items = data.items;
    const lookup = data.item_lookup;

    var replyLink: ReplyLink = .{ .zhtml = z };

    if (items.len == 0) {
        try layout.begin(.{ .title = "empty" });
        z.em.render("no items found");
        layout.end();
        return;
    }

    const op: HN.Item = items[0];
    replyLink.base_id = op.id;

    std.log.debug("rendering pages/item.zig with thread {d}", .{op.id});

    try layout.begin(.{ .title = op.title });

    if (op.parent == null) {
        z.div.attrs(.{
            .id = try sprintf(arena, "{d}", .{op.id}),
            .class = "op item",
        });
        z.div.@"<>"();
        {
            z.h1.@"<>"();
            {
                z.a.attr(.href, op.url orelse "#");
                z.a.render(op.title);
            }
            z.h1.@"</>"();

            const dt = try formatDateTime(arena, op.time);
            const name = try encodeName(arena, op, op.id);
            try z.print(
                arena,
                "{d} | submitted by {s} on {s} | {d} points {d} comments",
                .{
                    op.id,
                    name,
                    dt,
                    op.score,
                    items.len - 1,
                },
            );
            arena.free(name);
            arena.free(dt);

            z.br.render();
            if (op.url) |url| {
                z.a.attr(.href, url);
                z.a.render("[view article]");
            }
            z.a.attr(.id, "top");
            z.a.attr(.href, "#bottom");
            z.a.render("[go to bottom]");

            if (op.text.len > 0) {
                z.p.@"<>"();
                z.@"writeUnsafe!?"(op.text);
                z.p.@"</>"();
            }
        }
        z.div.@"</>"();
        z.br.@"<>"();
    } else {
        z.div.@"<>"();
        try z.a.attrf(arena, .href, "/item?id={d}", .{op.parent.?});
        if (op.thread_id) |tid| {
            z.a.render("[parent]");
            try z.a.attrf(arena, .href, "/item?id={d}", .{tid});
            z.a.render("[thread]");
            try z.print(arena, " {d} comments", .{items.len - 1});
        }
        z.div.@"</>"();
    }

    const start: usize = if (items[0].parent != null) 0 else 1;

    for (items[start..], 0..) |item, n| {
        z.div.attrs(.{
            .id = try sprintf(arena, "{d}", .{item.id}),
            .class = "item",
        });
        z.div.@"<>"();
        {
            z.div.attr(.class, "header");
            z.div.@"<>"();
            {
                z.div.@"<>"();
                {
                    z.div.attr(.class, try sprintf(arena, "margin depth-{d}", .{item.depth}));
                    z.div.render("");

                    const name = try encodeName(arena, item, replyLink.base_id);
                    try z.print(arena, "{d}. {s} {d}", .{
                        n + 1,
                        name,
                        item.id,
                    });
                    arena.free(name);

                    try z.a.attrf(arena, .href, "/item?id={d}", .{item.id});
                    z.a.render("[link]");

                    const dt = try formatDateTime(arena, item.time);
                    z.span.render(dt);
                    arena.free(dt);
                }
                z.div.@"</>"();

                {
                    //const parent = if (item.parent) |parent_id|
                    //    lookup.get(parent_id)
                    //else
                    //    null;

                    //const prev_sibling = blk: {
                    //    const p = parent orelse break :blk null;
                    //    if (item.sibling_num == 0) break :blk null;
                    //    const sib_id = p.kids[item.sibling_num - 1];
                    //    break :blk lookup.get(sib_id);
                    //};
                    //const next_sibling = blk: {
                    //    const p = parent orelse break :blk null;
                    //    if (item.sibling_num >= p.kids.len - 1) break :blk null;
                    //    const sib_id = p.kids[item.sibling_num + 1];
                    //    break :blk lookup.get(sib_id);
                    //};
                    //z.div.@"<>"();
                    //if (prev_sibling) |sib| {
                    //    z.a.attr(.href, try sprintf(arena, "#{d}", .{sib.id}));
                    //    z.a.attr(.title, "previous sibling comment");
                    //    z.a.render("<~");
                    //}
                    //if (next_sibling) |sib| {
                    //    z.a.attr(.href, try sprintf(arena, "#{d}", .{sib.id}));
                    //    z.a.attr(.title, "next sibling comment");
                    //    z.a.render("~>");
                    //}
                    //z.div.@"</>"();
                }
            }
            z.div.@"</>"();

            z.div.@"<>"();
            if (item.parent) |p| {
                if (lookup.get(p)) |parent| {
                    if (p == op.id) replyLink.attr(.class, "op");
                    try replyLink.render(arena, parent);
                    z.br.@"<>"();
                }
            }
            z.@"writeUnsafe!?"(item.text);
            z.div.@"</>"();

            if (item.kids.len > 0) {
                z.small.@"<>"();
                try z.print(arena, "replies({d}): ", .{item.kids.len});
                z.small.@"</>"();

                if (item.kids.len > 0) {
                    z.span.@"<>"();
                    for (item.kids) |rep_id| {
                        z.small.@"<>"();
                        replyLink.attr(.class, "resp");
                        try replyLink.render(arena, lookup.get(rep_id));
                        z.write(" ");
                        z.small.@"</>"();
                    }
                    z.span.@"</>"();
                }
            }
        }

        z.div.@"</>"();
    }
    z.a.attr(.id, "bottom");
    z.a.attr(.href, "#top");
    z.a.render("[go to top]");

    layout.end();

    return z.getError();
}

const ReplyLink = struct {
    zhtml: *Zhtml,
    base_id: HN.ItemID = 0,
    class: []const u8 = "",

    const AttrEnum = enum {
        class,
    };

    pub fn attr(self: *@This(), key: AttrEnum, value: []const u8) void {
        switch (key) {
            .class => self.class = value,
        }
    }

    fn clear(self: *@This()) void {
        self.class = "";
    }

    pub fn render(self: *@This(), arena: Allocator, item_arg: ?HN.Item) !void {
        defer self.clear();

        const item = item_arg orelse return;
        const z = self.zhtml;
        const name = try encodeName(arena, item, self.base_id);
        defer arena.free(name);

        z.a.attrs(.{
            .href = try sprintf(arena, "#{d}", .{item.id}),
            .class = try sprintf(arena, "replink {s}", .{
                self.class,
            }),
            .title = if (item.text.len < 200)
                item.text
            else
                try sprintf(arena, "{s}...", .{item.text[0..200]}),
        });

        z.a.@"<>"();
        try z.print(arena, ">>{s}", .{name});
        z.a.@"</>"();
    }
};

const encodeChars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn encodeName(arena: Allocator, item: HN.Item, base_id: u64) ![]const u8 {
    const by = item.by[0..@min(item.by.len, 6)];
    if (item.id == base_id) {
        return sprintf(arena, "{s}+(OP)", .{by});
    }

    // huh for some really strange reason, some items has an id < than parent or ancestor id
    // why does that happen? Maybe it's a post that was moved from elsewhere.
    const id = try encodeID(arena, if (item.id >= base_id) item.id - base_id else item.id);
    defer arena.free(id);
    return sprintf(arena, "{s}+{s}", .{ by, id });
}

fn encodeID(arena: Allocator, id: u64) ![]const u8 {
    var result: std.ArrayList(u8) = try .initCapacity(arena, 16);
    defer result.deinit(arena);

    var n = id;
    while (true) {
        const ch = encodeChars[n % encodeChars.len];
        try result.append(arena, ch);
        n /= encodeChars.len;
        if (n == 0) break;
    }

    return result.toOwnedSlice(arena);
}

fn u64AsString(n: u64) []const u8 {
    const arr: [8]u8 = @bitCast(n);
    return arr[0..4];
}

fn formatDateTime(arena: Allocator, time: i64) ![]const u8 {
    // TODO: use js to get client time zone
    //console.log(Intl.DateTimeFormat().resolvedOptions().timeZone)
    //document.cookie = "";
    // then store that in a cookie

    const inst = try zeit.instant(.{
        .source = .{ .unix_timestamp = time },
    });
    const t = inst.time();

    return std.fmt.allocPrint(arena, "{d}-{d:02}-{d:02} {d:02}:{d:02}:{d:02}", .{
        t.year,
        t.month,
        t.day,
        t.hour,
        t.minute,
        t.second,
    });
}
