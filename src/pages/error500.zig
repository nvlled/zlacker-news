const std = @import("std");

const Zhtml = @import("zhtml");

const Layout = @import("./layout.zig");

pub fn render(z: *Zhtml) !void {
    const layout: Layout = .{ .zhtml = z };
    try layout.begin(.{});
    z.h2.render("500 Internal server error");
    z.p.render("Something broke. Please reload the page to try again.");
    layout.end();

    return z.getError();
}
