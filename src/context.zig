const std = @import("std");
const HN = @import("./hn.zig");
const Zhtml = @import("zhtml");
const httpz = @import("httpz");

hn: *HN,
zhtml: *Zhtml,
req: *httpz.Request,
res: *httpz.Response,
allocator: std.mem.Allocator,
