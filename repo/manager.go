package repo

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// WorkDir returns the repo work directory (default: ~/.hub/repos).
func WorkDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("home dir: %w", err)
	}
	dir := filepath.Join(home, ".hub", "repos")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("mkdir repos: %w", err)
	}
	return dir, nil
}

// Clone clones a git repository into the work directory and returns the local path.
func Clone(url string) (string, error) {
	workDir, err := WorkDir()
	if err != nil {
		return "", err
	}

	// Derive a local directory name from the URL
	name := filepath.Base(url)
	if strings.HasSuffix(name, ".git") {
		name = name[:len(name)-4]
	}
	if name == "" || name == "." {
		name = "repo"
	}

	localPath := filepath.Join(workDir, name)

	// Check if already cloned
	if _, err := os.Stat(filepath.Join(localPath, ".git")); err == nil {
		// Already exists — pull latest
		if err := runGit(localPath, "pull", "--ff-only"); err != nil {
			return localPath, fmt.Errorf("already cloned but pull failed: %w", err)
		}
		return localPath, nil
	}

	if err := runGit("", "clone", url, localPath); err != nil {
		return "", fmt.Errorf("clone failed: %w", err)
	}

	return localPath, nil
}

// ReadFiles reads one or more files and returns their content keyed by path.
// Paths are resolved relative to the repo root, or used as-is if absolute.
func ReadFiles(paths []string) (map[string]string, error) {
	results := make(map[string]string, len(paths))
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			return results, fmt.Errorf("reading %s: %w", p, err)
		}
		results[p] = string(content)
	}
	return results, nil
}

// FormatFilesAsContext formats the read files into a prompt context block.
func FormatFilesAsContext(files map[string]string) string {
	if len(files) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("Here are the relevant file contents:\n\n")

	for path, content := range files {
		ext := filepath.Ext(path)
		sb.WriteString(fmt.Sprintf("─── %s ", path))
		sb.WriteString(strings.Repeat("─", max(60-len(path), 3)))
		sb.WriteString("\n")

		if ext != "" {
			sb.WriteString("```" + strings.TrimPrefix(ext, ".") + "\n")
		} else {
			sb.WriteString("```\n")
		}
		sb.WriteString(content)
		if !strings.HasSuffix(content, "\n") {
			sb.WriteString("\n")
		}
		sb.WriteString("```\n\n")
	}

	return sb.String()
}

// Status runs `git status --short` and returns the output.
func Status(repoPath string) (string, error) {
	out, err := outputGit(repoPath, "status", "--short")
	if err != nil {
		return "", err
	}
	return out, nil
}

// Diff runs `git diff` (unstaged) and returns the output.
func Diff(repoPath string) (string, error) {
	out, err := outputGit(repoPath, "diff")
	if err != nil {
		return "", err
	}
	return out, nil
}

// StagedDiff runs `git diff --cached` and returns the output.
func StagedDiff(repoPath string) (string, error) {
	out, err := outputGit(repoPath, "diff", "--cached")
	if err != nil {
		return "", err
	}
	return out, nil
}

// Log returns the last N commit messages (default 10).
func Log(repoPath string, n int) (string, error) {
	if n <= 0 {
		n = 10
	}
	out, err := outputGit(repoPath, "log", fmt.Sprintf("-%d", n), "--oneline")
	if err != nil {
		return "", err
	}
	return out, nil
}

// ApplyDiff applies a unified diff/patch to the repository.
func ApplyDiff(repoPath, patch string) error {
	cmd := exec.Command("git", "-C", repoPath, "apply")
	cmd.Stdin = strings.NewReader(patch)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("apply failed: %w\noutput: %s", err, string(out))
	}
	return nil
}

// CommitAll stages all changes and commits with the given message.
func CommitAll(repoPath, message string) error {
	// Stage all
	if err := runGit(repoPath, "add", "-A"); err != nil {
		return fmt.Errorf("git add failed: %w", err)
	}

	// Check if there's anything to commit
	status, _ := outputGit(repoPath, "status", "--porcelain")
	if strings.TrimSpace(status) == "" {
		return fmt.Errorf("nothing to commit")
	}

	if err := runGit(repoPath, "commit", "-m", message); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}

// Branch returns the current branch name.
func Branch(repoPath string) (string, error) {
	out, err := outputGit(repoPath, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// CurrentRepo returns the git repo root for the given path (or cwd).
func CurrentRepo(path string) (string, error) {
	if path == "" {
		path = "."
	}
	out, err := outputGit(path, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("not a git repository: %w", err)
	}
	return strings.TrimSpace(out), nil
}

// ShowFile shows the content of a file at a specific revision.
func ShowFile(repoPath, rev, filePath string) (string, error) {
	ref := rev
	if ref == "" {
		ref = "HEAD"
	}
	out, err := outputGit(repoPath, "show", ref+":"+filePath)
	if err != nil {
		return "", fmt.Errorf("git show %s:%s: %w", ref, filePath, err)
	}
	return out, nil
}

// ─── helpers ──────────────────────────────────────

func runGit(repoPath string, args ...string) error {
	cmdArgs := []string{"-C", repoPath}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command("git", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w\noutput: %s", strings.Join(args, " "), err, string(out))
	}
	return nil
}

func outputGit(repoPath string, args ...string) (string, error) {
	cmdArgs := []string{"-C", repoPath}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command("git", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w\noutput: %s", strings.Join(args, " "), err, string(out))
	}
	return string(out), nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
