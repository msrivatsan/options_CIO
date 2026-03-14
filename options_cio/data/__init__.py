from options_cio.data.feed_adapter import DataFeedAdapter, YFinanceFeed, get_feed

__all__ = ["DataFeedAdapter", "YFinanceFeed", "get_feed"]

# Lazy imports for tastytrade modules — only loaded when accessed
def __getattr__(name):
    if name == "TastytradeAdapter":
        from options_cio.data.tastytrade_adapter import TastytradeAdapter
        return TastytradeAdapter
    if name == "TastytradeStreamer":
        from options_cio.data.streamer import TastytradeStreamer
        return TastytradeStreamer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
