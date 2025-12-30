const std = @import("std");

const Zhtml = @import("zhtml");

const Layout = @import("./layout.zig");

pub fn render(z: *Zhtml) !void {
    const layout: Layout = .{ .zhtml = z };
    try layout.begin(.{});
    z.div.render("404 Page not found");
    layout.end();

    return z.getError();
}
