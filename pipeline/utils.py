from typing import Optional

from dagster import EnvVar


def get_table_preview(conn, table_name="raw_billing", limit=5):
    """Generate a markdown table preview for metadata."""
    try:
        # Get column names
        cols = conn.execute(f"SELECT * FROM {table_name} LIMIT 0").description
        col_names = [col[0] for col in cols]

        # Get data
        data = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()

        # Create markdown table
        md_table = "| " + " | ".join(col_names) + " |\n"
        md_table += "| " + " | ".join(["---"] * len(col_names)) + " |\n"

        for row in data:
            md_table += "| " + " | ".join([str(val) for val in row]) + " |\n"

        return md_table
    except Exception as e:
        return f"Error generating preview for {table_name}: {e}"


def get_env(env_var: str, default: Optional[str] = None) -> str:
    """
    Get environment variable using Dagster's EnvVar for runtime resolution.

    Args:
        env_var: Name of the environment variable
        default: Default value if variable is not found

    Returns:
        String value of the environment variable
    """
    try:
        # Use EnvVar.get_value() as per the correct usage pattern
        if default is not None:
            return EnvVar(env_var).get_value(default)
        return EnvVar(env_var).get_value()
    except ValueError:
        # If the environment variable is not found and no default is provided,
        # EnvVar.get_value() will raise a ValueError
        if default is not None:
            return default
        raise
