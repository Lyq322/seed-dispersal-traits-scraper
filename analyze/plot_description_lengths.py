"""
Script to plot the distribution of description text lengths.

python analyze/plot_description_lengths.py data/processed/descriptions_text_by_source.jsonl plots/description_lengths.png
"""

import json
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np


def get_description_lengths(jsonl_path):
    """
    Extract description lengths from JSONL file.

    Args:
        jsonl_path: Path to descriptions JSONL file with descriptions_text field

    Returns:
        List of description lengths (character counts)
    """
    jsonl_path = Path(jsonl_path)

    if not jsonl_path.exists():
        print(f"Error: File not found: {jsonl_path}")
        return []

    lengths = []
    word_counts = []

    print(f"Reading descriptions from {jsonl_path}...")

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                descriptions_text = record.get('descriptions_text')

                if descriptions_text:
                    # Character count
                    char_count = len(descriptions_text)
                    lengths.append(char_count)

                    # Word count (split by whitespace)
                    word_count = len(descriptions_text.split())
                    word_counts.append(word_count)

                # Progress indicator
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} records, "
                          f"found {len(lengths):,} descriptions...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator
    print(f"Found {len(lengths):,} descriptions with text")

    return lengths, word_counts


def plot_length_distribution(lengths, word_counts, output_path):
    """
    Plot the distribution of description lengths.

    Args:
        lengths: List of character counts
        word_counts: List of word counts
        output_path: Path to save the plot
    """
    if not lengths:
        print("Error: No description lengths to plot")
        return

    lengths = np.array(lengths)
    word_counts = np.array(word_counts)

    # Calculate statistics
    total_descriptions = len(lengths)
    mean_chars = np.mean(lengths)
    median_chars = np.median(lengths)
    max_chars = np.max(lengths)
    min_chars = np.min(lengths)

    mean_words = np.mean(word_counts)
    median_words = np.median(word_counts)
    max_words = np.max(word_counts)
    min_words = np.min(word_counts)

    print("\n" + "=" * 60)
    print("DESCRIPTION LENGTH STATISTICS")
    print("=" * 60)
    print(f"Total descriptions: {total_descriptions:,}")
    print()
    print("Character counts:")
    print(f"  Mean: {mean_chars:.0f}")
    print(f"  Median: {median_chars:.0f}")
    print(f"  Min: {min_chars}")
    print(f"  Max: {max_chars:,}")
    print()
    print("Word counts:")
    print(f"  Mean: {mean_words:.0f}")
    print(f"  Median: {median_words:.0f}")
    print(f"  Min: {min_words}")
    print(f"  Max: {max_words:,}")
    print("=" * 60)

    # Create figure with subplots (only histograms, no box plots)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Plot 1: Histogram of character counts
    ax1 = axes[0]
    # Use log scale for better visualization if there's a wide range
    if max_chars > 10000:
        bins = np.logspace(np.log10(min_chars + 1), np.log10(max_chars + 1), 50)
        ax1.hist(lengths, bins=bins, edgecolor='black', alpha=0.7)
        ax1.set_xscale('log')
        ax1.set_xlabel('Description Length (characters, log scale)', fontsize=11)
    else:
        bins = 50
        ax1.hist(lengths, bins=bins, edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Description Length (characters)', fontsize=11)
    ax1.set_ylabel('Number of Descriptions', fontsize=11)
    ax1.set_title('Distribution of Description Lengths (Characters)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)

    # Add statistics text
    stats_text = f'Total: {total_descriptions:,}\n'
    stats_text += f'Mean: {mean_chars:.0f}\n'
    stats_text += f'Median: {median_chars:.0f}\n'
    stats_text += f'Max: {max_chars:,}'
    ax1.text(0.98, 0.98, stats_text, transform=ax1.transAxes,
             fontsize=9, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    # Plot 2: Histogram of word counts
    ax2 = axes[1]
    if max_words > 1000:
        bins = np.logspace(np.log10(min_words + 1), np.log10(max_words + 1), 50)
        ax2.hist(word_counts, bins=bins, edgecolor='black', alpha=0.7, color='green')
        ax2.set_xscale('log')
        ax2.set_xlabel('Description Length (words, log scale)', fontsize=11)
    else:
        bins = 50
        ax2.hist(word_counts, bins=bins, edgecolor='black', alpha=0.7, color='green')
        ax2.set_xlabel('Description Length (words)', fontsize=11)
    ax2.set_ylabel('Number of Descriptions', fontsize=11)
    ax2.set_title('Distribution of Description Lengths (Words)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)

    # Add statistics text
    stats_text2 = f'Total: {total_descriptions:,}\n'
    stats_text2 += f'Mean: {mean_words:.0f}\n'
    stats_text2 += f'Median: {median_words:.0f}\n'
    stats_text2 += f'Max: {max_words:,}'
    ax2.text(0.98, 0.98, stats_text2, transform=ax2.transAxes,
             fontsize=9, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python plot_description_lengths.py <descriptions_jsonl> [output_plot]")
        print("Example: python plot_description_lengths.py data/processed/descriptions_text_by_source.jsonl plots/description_lengths.png")
        sys.exit(1)

    descriptions_jsonl_path = sys.argv[1]

    if len(sys.argv) >= 3:
        output_plot_path = sys.argv[2]
    else:
        # Default output path
        output_plot_path = Path("plots") / "description_lengths.png"

    output_plot_path = Path(output_plot_path)
    output_plot_path.parent.mkdir(parents=True, exist_ok=True)

    # Get description lengths
    lengths, word_counts = get_description_lengths(descriptions_jsonl_path)

    if not lengths:
        print("Error: No descriptions found")
        sys.exit(1)

    # Plot distribution
    plot_length_distribution(lengths, word_counts, output_plot_path)


if __name__ == "__main__":
    main()
