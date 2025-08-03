# Linux File Librarian - Enterprise Edition

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Enterprise Grade](https://img.shields.io/badge/grade-enterprise-green.svg)](https://github.com/gsamuele78/Linux-File-Librarian)

An enterprise-grade file management system designed with system engineer principles. Transforms chaotic file collections into clean, searchable, and intelligently organized libraries with minimal user intervention.

## 🚀 Quick Start

```bash
# Clone and install
git clone https://github.com/gsamuele78/Linux-File-Librarian.git
cd Linux-File-Librarian
./deployment/scripts/install.sh

# Configure (REQUIRED)
cp config/config.ini.example config/config.ini
nano config/config.ini  # Set your paths

# Process files
./deployment/scripts/run_professional.sh
```

## ✨ Key Features

### 🎯 Intelligent Classification
- **TTRPG Specialization**: Pathfinder, D&D, Italian RPGs (Giochi Uniti, Acheron Games)
- **Media Management**: Videos, audio, images with metadata extraction
- **Document Processing**: PDFs, academic papers, books with ISBN detection
- **Multi-tier Analysis**: Knowledge base, folder analysis, file type detection

### 🔄 Advanced Processing
- **Smart Deduplication**: Exact, content-based, and metadata matching
- **File Repair**: Automatic repair of corrupted PDFs, videos, archives
- **Internet Enrichment**: TMDB, OpenLibrary, CrossRef, arXiv, Wikipedia
- **NFO Generation**: MediaElch-style metadata files

### 🏢 Enterprise Features
- **Professional Orchestration**: Multi-threaded processing pipeline
- **Quality Metrics**: Comprehensive success tracking and reporting
- **Error Recovery**: Graceful handling of failures and network issues
- **Scalable Architecture**: Configurable workers and resource management

## 📁 Project Structure

```
Linux-File-Librarian/
├── src/                          # Source code
│   ├── core/                     # Core business logic
│   ├── services/                 # Domain services
│   ├── providers/                # External integrations
│   ├── enterprise/               # Enterprise features
│   ├── interfaces/               # Contract definitions
│   └── utils/                    # Utilities
├── config/                       # Configuration files
├── documentation/                # Comprehensive documentation
│   ├── architecture/             # Technical architecture
│   ├── api/                      # API documentation
│   ├── user/                     # User guides
│   └── developer/                # Developer resources
├── deployment/                   # Deployment resources
│   ├── scripts/                  # Installation scripts
│   ├── docker/                   # Container configuration
│   └── kubernetes/               # K8s manifests
├── tests/                        # Test suites
├── monitoring/                   # Observability
└── README.md
```

## 🛠 Installation Options

### Standard Installation
```bash
./deployment/scripts/install.sh
```
Core functionality with basic file organization.

### Enhanced Installation
```bash
./deployment/scripts/install_enhanced.sh
```
Adds media processing, document repair, internet enrichment, and enterprise features.

### Docker Deployment
```bash
docker build -f deployment/docker/Dockerfile -t linux-file-librarian:enterprise .
docker run -v /your/files:/app/data/input -v /your/library:/app/data/output linux-file-librarian:enterprise
```

### Kubernetes Deployment
```bash
kubectl apply -f deployment/kubernetes/
```

## 📖 Documentation

- **[User Guide](documentation/user/user-guide.md)**: Complete usage instructions
- **[Developer Guide](documentation/developer/developer-guide.md)**: Technical implementation details
- **[Architecture](documentation/architecture/ENTERPRISE_ARCHITECTURE.md)**: System design and patterns
- **[API Reference](documentation/developer/API_REFERENCE.md)**: Interface documentation

## 🎮 Gaming Content Support

### International Publishers
- **Paizo**: Pathfinder, Starfinder content
- **Wizards of the Coast**: D&D content via D&D Beyond
- **DriveThruRPG**: General TTRPG marketplace
- **Giochi Uniti**: Italian RPG publisher
- **Acheron Games**: Italian gaming content

### Automatic Classification
Files are automatically detected and organized by:
- Game system (Pathfinder, D&D 5e, etc.)
- Product type (Core Rulebooks, Adventures, Supplements)
- Publisher and edition
- Language and region

## 🌐 Internet Enrichment

### Free Providers (No API Keys)
- **CrossRef**: Academic papers and journals
- **arXiv**: Scientific preprints
- **Google Books**: Book metadata and covers
- **Wikipedia**: General knowledge
- **Internet Archive**: Historical documents

### Premium Providers (Free API Keys)
- **TMDB**: Movie and TV metadata
- **Fanart.tv**: High-quality artwork

## 🔧 Configuration

### Basic Setup
```ini
[Paths]
source_paths = /home/user/Downloads,/home/user/Documents
library_root = /home/user/OrganizedLibrary

[Processing]
max_workers = 4
enable_internet_enrichment = true
download_artwork = true
```

### Enterprise Features
```ini
[Repair]
enable_repair = true
repair_threshold = 0.6

[Deduplication]
enable_enhanced_dedup = true
dedup_strategies = exact,content,metadata

[InternetProviders]
enable_paizo = true
enable_giochi_uniti = true
enable_crossref = true
```

## 📊 Enterprise Reporting

After processing, get comprehensive reports:
```
=== ENTERPRISE PROCESSING REPORT ===
Files Processed: 1,247
Success Rate: 94.2%
Files Repaired: 23
Internet Enriched: 156
Duplicates Removed: 355
Processing Time: 12m 34s
Efficiency Rating: Excellent
```

## 🏗 Architecture Highlights

### Clean Architecture
- **Dependency Inversion**: High-level modules independent of low-level details
- **Interface Segregation**: Focused, cohesive interfaces
- **Single Responsibility**: Each module has one reason to change

### Enterprise Patterns
- **Repository Pattern**: Data access abstraction
- **Strategy Pattern**: Interchangeable algorithms
- **Factory Pattern**: Object creation abstraction
- **Observer Pattern**: Event-driven processing

### Quality Attributes
- **Scalability**: Multi-threaded, configurable processing
- **Reliability**: Comprehensive error handling and recovery
- **Maintainability**: Modular design with clear interfaces
- **Security**: Input validation, path traversal protection

## 🧪 Testing

```bash
# Unit tests
python -m pytest tests/unit/

# Integration tests
python -m pytest tests/integration/

# End-to-end tests
python -m pytest tests/e2e/
```

## 🤝 Contributing

1. **Fork** the repository
2. **Create** feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** changes (`git commit -m 'Add amazing feature'`)
4. **Push** to branch (`git push origin feature/amazing-feature`)
5. **Open** Pull Request

See [Developer Guide](documentation/developer/developer-guide.md) for detailed contribution guidelines.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](documentation/architecture/LICENSE.md) file for details.

## 🆘 Support

- **Documentation**: Comprehensive guides in `documentation/`
- **Issues**: [GitHub Issues](https://github.com/gsamuele78/Linux-File-Librarian/issues)
- **Discussions**: [GitHub Discussions](https://github.com/gsamuele78/Linux-File-Librarian/discussions)

## 🎯 Enterprise Use Cases

- **System Administrators**: Organize large file collections
- **TTRPG Enthusiasts**: Manage gaming content libraries
- **Media Collectors**: Organize movies, music, and documents
- **Academic Researchers**: Organize papers and research materials
- **Content Creators**: Manage digital asset libraries

---

**Linux File Librarian** - Transforming chaos into order with enterprise-grade reliability.