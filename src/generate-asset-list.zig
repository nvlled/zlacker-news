const std = @import("std");

const asset_dirname = "assets";

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    var arena: std.heap.ArenaAllocator = .init(debug_allocator.allocator());
    defer arena.deinit();

    const allocator = arena.allocator();

    std.debug.print("generating asset list\n", .{});

    var dest_file = try std.fs.cwd().createFile("src/assets.zig", .{
        .truncate = true,
    });
    defer dest_file.close();

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    var w = &buffer.writer;

    const files = try getFileList(allocator);
    try w.writeAll(
        \\// !! GENERATED FILE, DO NOT EDIT DIRECTLY OR ELSE??
        \\// This module contains all the files in the assets/ dir.
        \\// To regenerate, run: zig build gen-assetl
        \\
        \\
    );

    var maxlen: usize = 0;

    try w.writeAll("pub const Names = enum {\n");
    {
        for (files) |filename| {
            try w.print(
                \\    @"{s}",
                \\
            , .{filename});

            maxlen = @max(maxlen, filename.len);
        }
    }

    try w.writeAll("};");

    try w.writeAll("\n\n");

    try w.writeAll(
        \\pub fn getData(t: Names) []const u8 {
        \\    return switch (t) {
        \\
    );
    {
        for (files) |filename| {
            try w.print(
                \\        .@"{s}" => @embedFile("{s}"),
                \\
            , .{ filename, try std.fs.path.join(allocator, &.{ ".", filename }) });
        }
    }
    try w.writeAll(
        \\    };
        \\}
        \\
        \\
    );

    try w.print("pub const max_name_len = {d};\n", .{maxlen});

    try w.flush();
    const output = try buffer.toOwnedSlice();
    try dest_file.writeAll(output);

    std.debug.print("generated contents:\n{s}\n", .{output});
}

fn getFileList(allocator: std.mem.Allocator) ![]const []const u8 {
    var result: std.ArrayList([]const u8) = .{};

    var asset_dir = try std.fs.cwd().openDir("src/assets", .{
        .access_sub_paths = true,
        .iterate = true,
        .no_follow = true,
    });
    defer asset_dir.close();

    var iter = asset_dir.iterate();
    while (try iter.next()) |entry| {
        const filename = try std.fs.path.join(allocator, &.{ "/assets", entry.name });
        try result.append(allocator, filename);
    }

    return result.toOwnedSlice(allocator);
}
