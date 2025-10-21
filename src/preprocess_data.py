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
        List of dictionaries with game metadata and moves
    """
    game_data: list[dict[str, str]] = []
    moves = []

    lines: list[str] = list(lines)

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
    """
    Parses chess moves

    Args:
        moves_pre: Raw line of all moves

    Returns:
        List of tuple (round number, move)
    """
    moves: list[tuple[str, str]] = []

    # Remove last 3 characters with result '0-1' or '1-0'
    moves_pre = moves_pre[:-5].strip()
    for line in moves_pre.split("}"):
        if len(line.strip().split(" ")) <= 1:
            continue
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

    data = read_zst_file_streaming(file_path, max_lines=1000 * LINES_PER_GAME)
    parsed_data = parse_pgn_game(data)
    print(parsed_data)

    with open("data/preprocessed/merged.pgn", "w") as f:
        for data in parsed_data:
            _ = f.write(str(data) + ",\n")


if __name__ == "__main__":
    main()

