def find_column(report: bool, updated_values: bool):
    if report:
        return "report"
    if updated_values:
        return "updated_values"
    else:
        raise Exception("Invalid lock request, please specify column")
