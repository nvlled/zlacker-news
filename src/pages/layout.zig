const std = @import("std");
const Zhtml = @import("zhtml");
const RequestContext = @import("../context.zig");

ctx: *RequestContext,

pub fn begin_(self: @This()) !void {
    const z = self.ctx.zhtml;
    try z.html.begin_();
    try z.head.begin_();
    {
        try z.title.render_("zlacker");
        try z.meta.render(.{ .charset = "utf-8" });
    }
    try z.head.end();
    try z.body.begin_();

    try z.h1.render_("zlacker");
    try z.hr.render_();
}

pub fn end(self: @This()) !void {
    const z = self.ctx.zhtml;
    try z.body.end();
    try z.html.end();
}
