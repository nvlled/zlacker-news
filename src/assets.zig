// !! GENERATED FILE, DO NOT EDIT DIRECTLY OR ELSE??
// This module contains all the files in the assets/ dir.
// To regenerate, run: zig build gen-assetl

pub const Names = enum {
    @"/assets/script.js",
    @"/assets/style.css",
};

pub fn getData(t: Names) []const u8 {
    return switch (t) {
        .@"/assets/script.js" => @embedFile("./assets/script.js"),
        .@"/assets/style.css" => @embedFile("./assets/style.css"),
    };
}

pub fn link(t: Names) []const u8 {
    return @tagName(t);
}

pub const max_name_len = 17;
