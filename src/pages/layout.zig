const std = @import("std");
const Zhtml = @import("zhtml");
const RequestContext = @import("../context.zig");
const Assets = @import("../assets.zig");

ctx: *RequestContext,

pub const Attrs = struct {
    title: ?[]const u8 = null,
};

pub fn begin(self: @This(), attrs: Attrs) !void {
    const z = self.ctx.zhtml;

    z.@"writeUnsafe!?"("<!DOCTYPE html>");
    z.html.@"<>"();
    z.head.@"<>"();
    {
        z.title.@"<>"();
        z.write("zlacker");
        if (attrs.title) |title| if (title.len > 0) {
            z.write(" - ");
            z.write(title);
        };
        z.title.@"</>"();

        z.meta.attr(.charset, "utf-8");
        z.meta.render();

        z.link.attr(.rel, "stylesheet");
        z.link.attr(.href, Assets.link(.@"/assets/style.css"));
        z.link.render();
    }
    z.head.@"</>"();
    z.body.@"<>"();

    z.h1.@"<>"();
    z.a.attr(.href, "/");
    z.a.attr(.id, "top");
    z.a.render("zlacker");
    z.h1.@"</>"();
}

pub fn end(self: @This()) void {
    const z = self.ctx.zhtml;
    z.body.@"</>"();
    z.html.@"</>"();
}
