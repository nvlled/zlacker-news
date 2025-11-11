// !! GENERATED FILE, DO NOT EDIT DIRECTLY OR ELSE??
// This module contains all the files in the assets/ dir.
// To regenerate, run: zig build gen-assetl

pub const Names = enum {
    @"/assets/style.css",
    @"/assets/custom-nav.js",
};

pub fn getData(t: Names) []const u8 {
    return switch (t) {
        .@"/assets/style.css" => @embedFile("./assets/style.css"),
        .@"/assets/custom-nav.js" => @embedFile("./assets/custom-nav.js"),
    };
}

pub fn link(t: Names) []const u8 {
    return @tagName(t);
}

pub const max_name_len = 21;
