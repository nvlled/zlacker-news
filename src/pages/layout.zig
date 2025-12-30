const std = @import("std");
const Zhtml = @import("zhtml");
const RequestContext = @import("../context.zig");
const Assets = @import("../assets.zig");

zhtml: *Zhtml,

pub const Attrs = struct {
    title: ?[]const u8 = null,
};

pub fn begin(self: @This(), attrs: Attrs) !void {
    const z = self.zhtml;

    z.@"writeUnsafe!?"("<!DOCTYPE html>");
    z.html.@"<>"();
    z.head.@"<>"();
    {
        z.title.@"<>"();
        if (attrs.title) |title| if (title.len > 0) {
            z.write(title);
            z.write(" - ");
        };
        z.write("zlacker");
        z.title.@"</>"();

        z.meta.attr(.charset, "utf-8");
        z.meta.render();

        z.meta.attr(.name, "viewport");
        z.meta.attr(.content, "width=device-width,initial-scale=1");
        z.meta.render();

        z.link.attr(.rel, "stylesheet");
        z.link.attr(.href, Assets.link(.@"/assets/style.css"));
        z.link.render();

        z.script.attr(.src, Assets.link(.@"/assets/custom-nav.js"));
        z.script.render("");
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
    const z = self.zhtml;
    z.body.@"</>"();
    z.html.@"</>"();
}
