# restic-automator-rs

A Rust-based file system monitor that automatically triggers [restic](https://restic.net/) backups when changes are detected in configured directories.

## Features

- Real-time file system monitoring with automatic backup triggers
- Configurable throttling to prevent backup storms
- YAML-based configuration
- Comprehensive logging (both terminal and file-based)
- Automatic stale lock removal
- Support for multiple watch directories with individual settings

## Compatibility

⚠️ This project has currently only been tested on macOS. Compatibility with other operating systems is not guaranteed.

## Prerequisites

- Rust (latest stable version)
- [restic](https://restic.net/) backup utility installed and configured
- A configured restic repository

## Installation

```bash
# Clone the repository
git clone https://github.com/N0ne1eft/restic-automator-rs
cd restic-automator-rs

# Build the project
cargo build --release
```

## Configuration

Create a `config.yml` file with the following structure:

```yaml
repo: "your-restic-repository-path"
exclude-file: "path-to-exclude-file"
password-command: "command-to-get-repository-password"
logfile: "path-to-logfile"
env-path: "system-path"  # Optional - defaults to current system PATH
restic-path: "path-to-restic-binary"

dirs:
  - name: "backup-job-name"
    path: "/path/to/watch"
    throttle: 60  # Delay in seconds before triggering backup after changes

  # Add more directories as needed
  - name: "another-backup"
    path: "/another/path"
    throttle: 120
```

### Configuration Options

- `repo`: Your restic repository path (local path or remote URL)
- `exclude-file`: Path to the file containing patterns to exclude from backup
- `password-command`: Command that outputs your repository password
- `logfile`: Path where logs will be written
- `env-path`: (Optional) System PATH environment variable. If not specified, the current system PATH will be used
- `restic-path`: Path to the restic binary
- `dirs`: List of directories to monitor
  - `name`: Unique name for the backup job
  - `path`: Directory path to monitor
  - `throttle`: Delay in seconds before triggering a backup after changes are detected

## Usage

```bash
# Run with default config.yml in current directory
./restic-automator-rs

# Run with custom config file
./restic-automator-rs /path/to/config.yml
```

## Logging

The application logs both to the terminal and to the specified log file. Logs include:
- Backup initiation and completion
- Number of new and changed files
- Backup duration
- File system monitoring status
- Error messages and warnings

## Building from Source

```bash
# Debug build
cargo build

# Release build
cargo build --release
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 