"""
Script to plot the distribution of number of descriptions per species.
Includes species with 0 descriptions from world_flora_online_complete.jsonl.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np


def get_all_species_identifiers(complete_jsonl_path):
    """
    Get all species identifiers from the complete JSONL file.

    Args:
        complete_jsonl_path: Path to world_flora_online_complete.jsonl

    Returns:
        Set of species identifiers
    """
    complete_jsonl_path = Path(complete_jsonl_path)

    if not complete_jsonl_path.exists():
        print(f"Error: File not found: {complete_jsonl_path}")
        return set()

    species_identifiers = set()

    print(f"Reading species from {complete_jsonl_path}...")

    with open(complete_jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                page_type = record.get('page_type')

                # Only include species
                if page_type == 'species':
                    identifier = record.get('identifier')
                    if identifier:
                        species_identifiers.add(identifier)

                # Progress indicator
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} lines, found {len(species_identifiers):,} species...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator
    print(f"Found {len(species_identifiers):,} species in complete file")

    return species_identifiers


def count_descriptions_per_species(descriptions_jsonl_path):
    """
    Count descriptions per species identifier.

    Args:
        descriptions_jsonl_path: Path to descriptions JSONL file

    Returns:
        Dictionary mapping identifier to count of descriptions
    """
    descriptions_jsonl_path = Path(descriptions_jsonl_path)

    if not descriptions_jsonl_path.exists():
        print(f"Warning: Descriptions file not found: {descriptions_jsonl_path}")
        return defaultdict(int)

    description_counts = defaultdict(int)

    print(f"Counting descriptions from {descriptions_jsonl_path}...")

    with open(descriptions_jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                page_type = record.get('page_type')

                # Only count species descriptions
                if page_type == 'species':
                    identifier = record.get('identifier')
                    if identifier:
                        description_counts[identifier] += 1

                # Progress indicator
                if line_num % 10000 == 0:
                    print(f"  Processed {line_num:,} description records...", end='\r')

            except json.JSONDecodeError as e:
                print(f"\nWarning: Error parsing line {line_num}: {e}")
                continue

    print()  # New line after progress indicator
    print(f"Found descriptions for {len(description_counts):,} species")

    return description_counts


def plot_distribution(species_identifiers, description_counts, output_path):
    """
    Plot the distribution of number of descriptions per species.

    Args:
        species_identifiers: Set of all species identifiers
        description_counts: Dictionary mapping identifier to count
        output_path: Path to save the plot
    """
    # Count descriptions for each species (0 if not in description_counts)
    counts = []
    for identifier in species_identifiers:
        count = description_counts.get(identifier, 0)
        counts.append(count)

    counts = np.array(counts)

    # Calculate statistics
    total_species = len(counts)
    species_with_0 = np.sum(counts == 0)
    species_with_1_plus = np.sum(counts > 0)
    max_descriptions = np.max(counts)
    mean_descriptions = np.mean(counts)
    median_descriptions = np.median(counts)

    print("\n" + "=" * 60)
    print("DESCRIPTION DISTRIBUTION STATISTICS")
    print("=" * 60)
    print(f"Total species: {total_species:,}")
    print(f"Species with 0 descriptions: {species_with_0:,} ({100*species_with_0/total_species:.1f}%)")
    print(f"Species with 1+ descriptions: {species_with_1_plus:,} ({100*species_with_1_plus/total_species:.1f}%)")
    print(f"Max descriptions per species: {max_descriptions}")
    print(f"Mean descriptions per species: {mean_descriptions:.2f}")
    print(f"Median descriptions per species: {median_descriptions:.1f}")
    print("=" * 60)

    # Create figure with subplots
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    # Plot 1: Histogram of all counts (including 0)
    ax1 = axes[0]
    bins = np.arange(-0.5, max_descriptions + 1.5, 1)
    ax1.hist(counts, bins=bins, edgecolor='black', alpha=0.7)
    ax1.set_xlabel('Number of Descriptions per Species', fontsize=12)
    ax1.set_ylabel('Number of Species', fontsize=12)
    ax1.set_title('Distribution of Descriptions per Species (Including 0)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(0, min(max_descriptions + 1, 21)))  # Show up to 20, or max if less

    # Add statistics text
    stats_text = f'Total: {total_species:,} species\n'
    stats_text += f'Mean: {mean_descriptions:.2f}\n'
    stats_text += f'Median: {median_descriptions:.1f}\n'
    stats_text += f'Max: {max_descriptions}'
    ax1.text(0.98, 0.98, stats_text, transform=ax1.transAxes,
             fontsize=10, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    # Plot 2: Histogram excluding 0 (log scale for y-axis if needed)
    ax2 = axes[1]
    counts_with_descriptions = counts[counts > 0]
    if len(counts_with_descriptions) > 0:
        bins2 = np.arange(0.5, max_descriptions + 1.5, 1)
        ax2.hist(counts_with_descriptions, bins=bins2, edgecolor='black', alpha=0.7, color='green')
        ax2.set_xlabel('Number of Descriptions per Species', fontsize=12)
        ax2.set_ylabel('Number of Species', fontsize=12)
        ax2.set_title('Distribution of Descriptions per Species (Excluding 0)', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        ax2.set_xticks(range(1, min(max_descriptions + 1, 21)))

        # Add statistics text
        mean_with_desc = np.mean(counts_with_descriptions)
        median_with_desc = np.median(counts_with_descriptions)
        stats_text2 = f'Species with descriptions: {len(counts_with_descriptions):,}\n'
        stats_text2 += f'Mean: {mean_with_desc:.2f}\n'
        stats_text2 += f'Median: {median_with_desc:.1f}\n'
        stats_text2 += f'Max: {max_descriptions}'
        ax2.text(0.98, 0.98, stats_text2, transform=ax2.transAxes,
                 fontsize=10, verticalalignment='top', horizontalalignment='right',
                 bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")


def main():
    """Main function."""
    if len(sys.argv) < 3:
        print("Usage: python plot_descriptions_distribution.py <complete_jsonl> <descriptions_jsonl> [output_plot]")
        print("Example: python plot_descriptions_distribution.py data/raw/world_flora_online_complete.jsonl data/processed/descriptions.jsonl plots/descriptions_distribution.png")
        sys.exit(1)

    complete_jsonl_path = sys.argv[1]
    descriptions_jsonl_path = sys.argv[2]

    if len(sys.argv) >= 4:
        output_plot_path = sys.argv[3]
    else:
        # Default output path
        output_plot_path = Path("plots") / "descriptions_distribution.png"

    output_plot_path = Path(output_plot_path)
    output_plot_path.parent.mkdir(parents=True, exist_ok=True)

    # Get all species identifiers
    species_identifiers = get_all_species_identifiers(complete_jsonl_path)

    if not species_identifiers:
        print("Error: No species found in complete file")
        sys.exit(1)

    # Count descriptions per species
    description_counts = count_descriptions_per_species(descriptions_jsonl_path)

    # Plot distribution
    plot_distribution(species_identifiers, description_counts, output_plot_path)


if __name__ == "__main__":
    main()
