# Internet Enrichment Guide

The Enhanced Classification Engine provides internet-based metadata enrichment similar to MediaElch, automatically downloading rich metadata and artwork for your media files.

## Overview

The system integrates with multiple online databases to enhance your files with:

- **Rich Metadata**: Plot summaries, cast, crew, ratings, genres
- **High-Quality Artwork**: Posters, fanart, banners from Fanart.tv
- **Accurate Classification**: Professional-grade content identification
- **Automatic Caching**: Reduces API calls and improves performance

## Supported Providers

### The Movie Database (TMDB)
- **Content**: Movies, TV shows, documentaries
- **Metadata**: Plot, cast, crew, ratings, release dates, genres
- **Artwork**: Posters, backdrops, logos
- **API Key**: Free registration required
- **Rate Limits**: 40 requests per 10 seconds

### Open Library
- **Content**: Books, academic papers, documents
- **Metadata**: Authors, publishers, subjects, descriptions
- **Artwork**: Book covers
- **API Key**: Not required
- **Rate Limits**: Reasonable use policy

### Fanart.tv
- **Content**: Movies, TV shows, music
- **Artwork**: High-resolution posters, fanart, banners, logos
- **API Key**: Free registration recommended
- **Rate Limits**: 2000 requests per day (free tier)

## Configuration

### 1. Get API Keys

**TMDB API Key:**
1. Visit https://www.themoviedb.org/settings/api
2. Create free account
3. Request API key (instant approval)
4. Copy the "API Key (v3 auth)"

**Fanart.tv API Key:**
1. Visit https://fanart.tv/get-an-api-key/
2. Create free account
3. Generate personal API key
4. Copy the API key

### 2. Configure Settings

Edit `conf/config.ini`:

```ini
[Processing]
# Enable internet metadata enrichment
enable_internet_enrichment = true

# Download artwork/fanart
download_artwork = true

[APIKeys]
# The Movie Database API key
tmdb_api_key = your_tmdb_api_key_here

# Fanart.tv API key (optional but recommended)
fanart_api_key = your_fanart_api_key_here
```

## How It Works

### 1. Content Analysis
The system analyzes your files to generate search queries:
- Extracts titles from metadata when available
- Cleans filenames (removes quality tags, years, etc.)
- Uses intelligent pattern matching

### 2. Provider Search
For each file, the system:
- Searches relevant providers based on content type
- Scores results for best match selection
- Handles multiple result disambiguation

### 3. Metadata Enrichment
Retrieved metadata includes:
- **Movies**: Plot, cast, director, genre, rating, runtime
- **TV Shows**: Episode info, series details, network
- **Books**: Author, publisher, subjects, description

### 4. Artwork Download
When enabled, downloads:
- **Posters**: Movie/book covers
- **Fanart**: Background artwork
- **Banners**: Wide format artwork
- **Thumbnails**: Small preview images

### 5. Caching
- Metadata cached for 24 hours
- Reduces API calls for duplicate searches
- Improves processing speed

## File Organization

Enhanced metadata improves organization:

```
Library/
├── Media/
│   ├── Movies/
│   │   ├── Action/
│   │   │   ├── 2023/
│   │   │   │   ├── Movie Title (2023)/
│   │   │   │   │   ├── movie.mp4
│   │   │   │   │   ├── movie.nfo
│   │   │   │   │   └── artwork/
│   │   │   │   │       ├── movie-poster.jpg
│   │   │   │   │       ├── movie-fanart.jpg
│   │   │   │   │       └── movie-banner.jpg
```

## NFO File Enhancement

Internet-enriched NFO files contain comprehensive metadata:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<movie>
    <title>The Matrix</title>
    <originaltitle>The Matrix</originaltitle>
    <year>1999</year>
    <plot>A computer programmer discovers reality is a simulation...</plot>
    <tagline>Welcome to the Real World</tagline>
    <runtime>136</runtime>
    <genre>Action</genre>
    <genre>Sci-Fi</genre>
    <director>Lana Wachowski</director>
    <director>Lilly Wachowski</director>
    <actor>
        <name>Keanu Reeves</name>
        <role>Neo</role>
    </actor>
    <rating>8.7</rating>
    <votes>1500000</votes>
    <imdbid>tt0133093</imdbid>
    <tmdbid>603</tmdbid>
</movie>
```

## Performance Considerations

### API Rate Limits
- **TMDB**: 40 requests per 10 seconds
- **Fanart.tv**: 2000 requests per day (free)
- **Open Library**: No strict limits

### Processing Speed
- **With Internet**: 2-5 files/second (depends on API response)
- **Cached Results**: 10-20 files/second
- **Offline Mode**: 20-50 files/second

### Network Requirements
- **Bandwidth**: ~1-5MB per enriched file (including artwork)
- **Latency**: Works with high-latency connections
- **Offline**: Gracefully degrades to local classification

## Troubleshooting

### Common Issues

**No metadata found:**
- Check API keys are correctly configured
- Verify internet connection
- Check file naming (clean titles work better)
- Review search query generation in debug logs

**API rate limits exceeded:**
- Reduce max_workers in config
- Enable caching (default)
- Consider upgrading to paid API tiers

**Artwork download fails:**
- Check disk space availability
- Verify write permissions to library directory
- Check firewall/proxy settings

**Poor match quality:**
- Enable debug logging to see search queries
- Consider manual filename cleanup
- Check for special characters in filenames

### Debug Mode

Enable detailed logging in `conf/config.ini`:

```ini
[General]
debug = true
```

Debug output includes:
- Search queries generated
- API responses received
- Match scoring details
- Cache hit/miss information
- Artwork download status

## Best Practices

### File Naming
For best results, use clean filenames:
- **Good**: `The Matrix (1999).mp4`
- **Better**: `The Matrix.mp4` (year detected from metadata)
- **Avoid**: `The.Matrix.1999.1080p.BluRay.x264-GROUP.mp4`

### API Key Management
- Keep API keys secure and private
- Don't commit keys to version control
- Consider environment variables for deployment
- Monitor usage against rate limits

### Performance Optimization
- Enable caching (default)
- Use appropriate max_workers setting
- Process in batches for large collections
- Consider running during off-peak hours

### Quality Control
- Review enriched metadata periodically
- Use debug mode to verify match accuracy
- Manually correct mismatched content
- Report issues to improve matching algorithms

## Integration with Media Centers

### Kodi
- NFO files are fully compatible
- Artwork follows Kodi naming conventions
- Metadata fields map to Kodi database

### Plex
- Directory structure works with Plex agents
- NFO files can supplement Plex metadata
- Artwork enhances Plex presentation

### Jellyfin
- Compatible with Jellyfin NFO format
- Supports custom metadata fields
- Artwork integrates with Jellyfin themes

## Privacy and Data Usage

### Data Collection
- No personal data sent to providers
- Only file titles/names used for searches
- No file content uploaded or analyzed

### API Usage
- Respects provider terms of service
- Implements proper rate limiting
- Uses official API endpoints only

### Local Storage
- Metadata cached locally only
- No cloud storage of personal data
- Cache can be cleared anytime

## Future Enhancements

Planned improvements:
- **Additional Providers**: MusicBrainz, IGDB, BoardGameGeek
- **Machine Learning**: Improved match scoring
- **Batch Processing**: Bulk metadata updates
- **Custom Providers**: User-defined metadata sources
- **Metadata Editing**: GUI for manual corrections