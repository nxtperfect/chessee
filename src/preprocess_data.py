"""
Get file from data/
then check what is the format per each game
only read games of rating > 1400
make sure to not overflow the memory (8gb batches)
save up correct games into different file named "<year><month>_preprocessed_games_<amount_of_games>.pgn.zst"
after that, make sure to compress the file and move it to data/preprocessed/
"""

import os
import io
import math
import zstandard as zstd
from typing import Iterator, TextIO
import re


def read_zst_file_streaming(file_path: str, max_lines: int = 500) -> Iterator[str]:
    """
    Alternative streaming approach that reads line by line without loading entire file.
    This is more memory efficient for very large files.

    Args:
        file_path: Path to the .zst file
        max_lines: Maximum number of lines to read

    Yields:
        Individual lines from the file
    """
    with open(file_path, "rb") as compressed_file:
        dctx = zstd.ZstdDecompressor()

        with dctx.stream_reader(compressed_file) as reader:
            text_reader = io.TextIOWrapper(
                reader, encoding="utf-8", errors="strict", newline=""
            )

            for i, line in enumerate(text_reader):
                if i >= max_lines:
                    break
                yield line.rstrip("\n\r")


def parse_pgn_game(lines: Iterator[str]) -> list[dict[str, str]]:
    """
    Parse a PGN game from a list of lines.

    Args:
        lines: List of lines representing a game

    Returns:
        Dictionary with game metadata and moves
    """
    game_data: list[dict[str, str]] = []
    moves = []

    lines: list[str] = list(lines)

    # treat these values as offsets per each game
    # line 19 is moves
    # line 20 is empty
    # line 21 is next game
    for i in range(math.ceil(len(lines) / 20)):
        current_game_data = {}
        current_line = lines[i * 20 : (i + 1) * 20]
        current_line = [s.replace("[", "").replace("]", "") for s in current_line]
        current_game_data["result"] = (
            current_line[6].removeprefix("Result ").replace('"', "").strip()
        )
        current_game_data["white_elo"] = (
            current_line[9].removeprefix("WhiteElo ").replace('"', "").strip()
        )
        current_game_data["black_elo"] = (
            current_line[10].removeprefix("BlackElo ").replace('"', "").strip()
        )
        current_game_data["opening"] = (
            current_line[14].removeprefix("Opening ").replace('"', "").strip()
        )
        print(current_line[18])
        current_game_data["moves"] = parse_moves(current_line[18])
        game_data.append(current_game_data)
    return game_data


def parse_moves(moves_pre: str):
    # Sample
    # 1. e4 { [%clk 0:02:00] } 1... c5 { [%clk 0:02:00] } 2. Nc3 { [%clk 0:01:59] } 2... d6 { [%clk 0:02:00] }
    # remove {} and all inside
    # get the \d. and \d...
    # then get first element from splitting with spaces
    moves: list[tuple[str, str]] = []

    # Remove last 3 characters with result '0-1' or '1-0'
    moves_pre = moves_pre[:-5].strip()
    for line in moves_pre.split("}"):
        round = line.strip().split(".")[0]
        move = line.strip().split(" ")[1]
        moves.append((round, move))
    return moves


def filter_games_by_rating(game_data: dict, min_rating: int = 1400) -> bool:
    """
    Check if a game meets the minimum rating requirement.

    Args:
        game_data: Dictionary containing game metadata
        min_rating: Minimum rating threshold

    Returns:
        True if game meets rating requirement
    """
    try:
        # Check both WhiteElo and BlackElo
        white_elo = int(game_data.get("WhiteElo", "0"))
        black_elo = int(game_data.get("BlackElo", "0"))

        return white_elo >= min_rating and black_elo >= min_rating
    except (ValueError, TypeError):
        return False


def main():
    file_path = "data/lichess_db_standard_rated_2022-12.pgn.zst"

    if not os.path.exists(file_path):
        print(f"File {file_path} not found!")
        return

    print("Reading .zst file in chunks of 500 lines...")

    print("Alternative: Streaming line by line")
    print("=" * 50)

    LINES_PER_GAME = 20

    data = read_zst_file_streaming(file_path, max_lines=4 * LINES_PER_GAME)
    parsed_data = parse_pgn_game(data)
    print(parsed_data)

    with open("data/preprocessed/merged.pgn", "w") as f:
        for data in parsed_data:
            _ = f.write(str(data) + ",\n")


if __name__ == "__main__":
    main()

