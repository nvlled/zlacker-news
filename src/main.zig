const std = @import("std");
const zlacker_news = @import("zlacker_news");

pub fn main() !void {
    try zlacker_news.startServer();
}
