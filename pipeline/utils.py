# Helper function to get table preview for metadata
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
