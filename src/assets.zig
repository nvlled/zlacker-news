// !! GENERATED FILE, DO NOT EDIT DIRECTLY OR ELSE??
// This module contains all the files in the assets/ dir.
// To regenerate, run: zig build gen-assetl

pub const Names = enum {
    @"script.js",
    @"style.css",
};

pub const Data = .{
    .@"script.js" = @embedFile("assets/script.js"),
    .@"style.css" = @embedFile("assets/style.css"),
};

pub fn getData(t: Names) []const u8 {
    return switch (t) {
        .@"script.js" => Data.@"script.js",
        .@"style.css" => Data.@"style.css",
    };
}
