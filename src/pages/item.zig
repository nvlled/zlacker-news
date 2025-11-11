const std = @import("std");
const zeit = @import("zeit");
const Allocator = std.mem.Allocator;
const sprintf = std.fmt.allocPrint;
const HN = @import("../hn.zig");
const Zhtml = @import("zhtml");
const Layout = @import("./layout.zig");
const Assets = @import("../assets.zig");
const RequestContext = @import("../context.zig");
const rrrr = @import("rrrr");

pub const Data = struct {
    items: []const HN.Item,
    item_lookup: *std.AutoHashMapUnmanaged(HN.ItemID, HN.Item),
    discussion_mode: bool = false,
    with_links_only: bool = false,
};

const any_link: rrrr.RE = &.concat(&.{
    &.literal("http"),
    &.optional(&.literal("s")),
    &.literal(":&#x2F;&#x2F;"),
});

const hn_link: rrrr.RE = &.concat(&.{
    &.literal(
        \\<a href="https:&#x2F;&#x2F;news.ycombinator.com&#x2F;item?id=
    ),
    &.capture(&.oneOrMore(&.digit)),
    &.literal(
        \\">
    ),
    &.@"oneOrMore?"(&.exceptChar("<>")),
    &.literal("</a>"),
});

const depth_symbols: []const []const u8 = &.{
    "◧", "◩", "◪", "◨", "⬒", "⬓", "⬔", "⧯",
    "▣", "▦", "▧", "▨", "◲", "◳", "⚿", "⛋",
    "⬕", "⬚", "⧄", "⧅", "⧆", "⧇", "⧈", "⧉",
    "⧠", "⧮", "⛝", "⛞", "⛶", "⛾", "⟎", "⟏",
    "⟤", "⟥", "⯀", "⯐", "■",
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

    z.div.attrs(.{
        .id = try sprintf(arena, "{d}", .{op.id}),
        .class = "op item",
    });
    z.div.@"<>"();

    if (data.discussion_mode) {
        z.div.@"<>"();
        try z.a.attrf(arena, .href, "/item?id={d}", .{op.id});
        try z.a.renderf(arena, "[return to \"{s}\"]", .{op.title});
        z.div.@"</>"();
    } else if (op.parent == null) {
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
            "submitted by {s} on {s} | {d} points {d} comments",
            .{
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
        try z.a.attrf(arena, .href, "https://news.ycombinator.com/item?id={d}", .{op.id});
        z.a.render("[source]");

        if (!data.with_links_only) {
            try z.a.attrf(arena, .href, "/item?id={d}&links=ye", .{op.id});
            z.a.render("[links]");
        }

        z.a.attr(.href, "#bottom");
        z.a.render("[go to bottom]");

        if (op.text.len > 0) {
            z.p.@"<>"();
            z.@"writeUnsafe!?"(op.text);
            z.p.@"</>"();
        }

        z.div.@"<>"();
        if (op.kids.len > 0 and !data.with_links_only) {
            z.small.@"<>"();
            try z.print(arena, "replies({d}): ", .{op.kids.len});
            z.small.@"</>"();

            z.span.attr(.class, "reply-links");
            z.span.@"<>"();
            for (op.kids) |rep_id| {
                z.small.@"<>"();
                replyLink.attr(.class, "resp");
                try replyLink.render(arena, lookup.get(rep_id));
                z.write(" ");
                z.small.@"</>"();
            }
            z.span.@"</>"();
        }
        z.div.@"</>"();
    } else {
        if (op.thread_id) |tid| {
            try z.a.attrf(arena, .href, "/item?id={d}", .{op.parent.?});
            z.a.render("[parent]");
            try z.a.attrf(arena, .href, "/item?id={d}", .{tid});
            z.a.render("[thread]");
            try z.print(arena, " {d} comments", .{items.len - 1});
        }
    }

    if (data.with_links_only) {
        z.div.@"<>"();
        z.br.render();
        z.em.render("NOTE: showing posts with links only");
        try z.a.attrf(arena, .href, "/item?id={d}", .{op.id});
        z.a.render("show all posts");
        z.div.@"</>"();
    }

    z.div.@"</>"();

    const start: usize = if (items[0].parent != null) 0 else 1;

    for (items[start..], 0..) |item, n| {
        if (data.with_links_only and !try any_link.matches(arena, item.text, .{})) {
            continue;
        }

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
                    z.div.attr(.class, "depth");
                    try z.div.attrf(arena, .title, "post depth: {d}", .{item.depth});
                    z.div.@"<>"();
                    for (0..item.depth) |i| {
                        z.write(depth_symbols[i % depth_symbols.len]);
                    }
                    z.div.@"</>"();

                    const name = try encodeName(arena, item, replyLink.base_id);
                    try z.print(arena, "{d}. {s}", .{
                        n + 1,
                        name,
                    });
                    arena.free(name);

                    try z.a.attrf(arena, .href, "/item?id={d}", .{item.id});
                    z.a.render("[view]");
                    try z.a.attrf(arena, .href, "https://news.ycombinator.com/item?id={d}", .{item.id});
                    z.a.render("[source]");
                    if (item.depth >= 2 and item.thread_id != null and !data.discussion_mode) {
                        try z.a.attrf(arena, .href, "/item/discussion?id={d}&tid={d}", .{ item.id, item.thread_id.? });
                        z.a.render("[discussion]");
                    }

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

            z.attr(.class, "body");
            z.div.@"<>"();
            if (item.parent) |p| {
                if (lookup.get(p)) |parent| {
                    if (p == op.id) replyLink.attr(.class, "op");
                    try replyLink.render(arena, parent);
                    z.br.@"<>"();
                }
            }

            {
                var link_search = try hn_link.searchAll(ctx.allocator, item.text, .{});
                defer link_search.deinit(ctx.allocator);

                while (link_search.next(ctx.allocator)) |m| {
                    z.@"writeUnsafe!?"(link_search.skippedString(item.text));
                    if (m.capture) |cap| {
                        try z.a.attrf(arena, .href, "/item?id={s}", .{cap.string(item.text)});
                        try z.a.renderf(arena, ">>{s}", .{cap.string(item.text)});
                    }
                }
                z.@"writeUnsafe!?"(link_search.skippedString(item.text));
            }

            z.div.@"</>"();

            if (item.kids.len > 0 and !data.with_links_only) {
                z.small.@"<>"();
                try z.print(arena, "replies({d}): ", .{item.kids.len});
                z.small.@"</>"();

                z.span.attr(.class, "reply-links");
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

    const tag_re: rrrr.Regex = .concat(&.{
        &.literal("<"),
        &.optional(&.literal("/")),
        &.@"oneOrMore?"(&.exceptChar("<>")),
        &.literal(">"),
    });

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

        var title = item.text;
        title = try tag_re.replaceAll(arena, title, "~", .{});

        z.a.attrs(.{
            .href = try sprintf(arena, "#{d}", .{item.id}),
            .class = try sprintf(arena, "replink {s}", .{
                self.class,
            }),
            .title = title[0..@min(300, title.len)],
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
