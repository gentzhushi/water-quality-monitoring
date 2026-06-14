import html


def get_value(alert, name, default="unknown"):
    value = alert.get(name)
    if value is None or value == "":
        return default
    return str(value)


def escape_text(value):
    return html.escape(str(value), quote=True)


def code_text(value):
    return f"<code>{escape_text(value)}</code>"


def reading_text(alert):
    parameter = get_value(alert, "parameter")
    value = get_value(alert, "value")
    unit = get_value(alert, "unit", "")

    if unit:
        return f"{parameter}: {value} {unit}"

    return f"{parameter}: {value}"


def threshold_text(alert):
    unit = get_value(alert, "unit", "")
    low = alert.get("threshold_low")
    high = alert.get("threshold_high")

    if low is not None and high is not None:
        text = f"{low} to {high}"
    elif low is not None:
        text = f"at least {low}"
    elif high is not None:
        text = f"at most {high}"
    else:
        return "unknown"

    if unit:
        return f"{text} {unit}"

    return text


def location_text(alert):
    location_name = get_value(alert, "location_name")
    location_id = get_value(alert, "location_id", "")

    if location_id and location_id != "unknown":
        return f"{location_name} ({location_id})"

    return location_name


def build_summary_section(alert):
    return "\n".join(
        [
            "<b>Summary</b>",
            f"Severity: {code_text(get_value(alert, 'severity'))}",
            f"Alert type: {code_text(get_value(alert, 'alert_type'))}",
            f"Reading: {code_text(reading_text(alert))}",
            f"Expected range: {code_text(threshold_text(alert))}",
        ]
    )


def build_location_section(alert):
    return "\n".join(
        [
            "<b>Location</b>",
            escape_text(location_text(alert)),
            f"Sensor: {code_text(get_value(alert, 'sensor_id'))}",
        ]
    )


def build_timing_section(alert):
    return "\n".join(
        [
            "<b>Timing</b>",
            f"Event time: {code_text(get_value(alert, 'event_time'))}",
            f"Processed at: {code_text(get_value(alert, 'processed_at'))}",
        ]
    )


def build_telegram_message(alert):
    return "\n".join(
        [
            "<b>Water Quality Alert</b>",
            "",
            f"<blockquote>{escape_text(get_value(alert, 'message'))}</blockquote>",
            "",
            build_summary_section(alert),
            "",
            build_location_section(alert),
            "",
            build_timing_section(alert),
        ]
    )
