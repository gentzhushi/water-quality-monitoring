import html


def get_value(alert, name, default="unknown"):
    value = alert.get(name)
    if value is None or value == "":
        return default

    return value


def escape(value):
    return html.escape(str(value), quote=True)


def reading_text(alert):
    value = get_value(alert, "value")
    unit = get_value(alert, "unit", "")

    if unit:
        return f"{value} {unit}"

    return str(value)


def threshold_text(alert):
    low = alert.get("threshold_low")
    high = alert.get("threshold_high")

    if low is not None and high is not None:
        return f"expected range: {low} - {high}"

    if low is not None:
        return f"minimum allowed: {low}"

    if high is not None:
        return f"maximum allowed: {high}"

    return "threshold not provided"


def severity_style(alert):
    severity = str(get_value(alert, "severity", "unknown")).lower()

    if severity == "critical":
        return {
            "label": "critical",
            "accent": "#dc2626",
            "accent_dark": "#991b1b",
            "soft": "#fef2f2",
            "text": "#7f1d1d",
        }

    if severity == "warning":
        return {
            "label": "warning",
            "accent": "#d97706",
            "accent_dark": "#92400e",
            "soft": "#fffbeb",
            "text": "#78350f",
        }

    return {
        "label": severity,
        "accent": "#64748b",
        "accent_dark": "#334155",
        "soft": "#f8fafc",
        "text": "#334155",
    }


def build_subject(alert):
    alert_type = get_value(alert, "alert_type")
    location_name = get_value(alert, "location_name")
    return f"Water quality alert: {alert_type} at {location_name}"


def build_text(alert):
    lines = [
        "Water quality alert",
        "",
        f"Alert type: {get_value(alert, 'alert_type')}",
        f"Severity: {get_value(alert, 'severity')}",
        f"Sensor: {get_value(alert, 'sensor_id')}",
        f"Location: {get_value(alert, 'location_name')} ({get_value(alert, 'location_id')})",
        f"Parameter: {get_value(alert, 'parameter')}",
        f"Reading: {reading_text(alert)}",
        f"Threshold: {threshold_text(alert)}",
        f"Event time: {get_value(alert, 'event_time')}",
        f"Processed at: {get_value(alert, 'processed_at')}",
        "",
        str(get_value(alert, "message", "")),
    ]

    return "\n".join(lines)


def summary_cell(label, value):
    return (
        '<td style="padding:12px 10px; width:50%;">'
        '<div style="font-size:12px; color:#64748b; text-transform:uppercase; '
        'letter-spacing:0.04em; margin-bottom:5px;">'
        f"{escape(label)}</div>"
        '<div style="font-size:18px; line-height:24px; color:#0f172a; '
        'font-weight:700;">'
        f"{escape(value)}</div>"
        "</td>"
    )


def detail_row(label, value):
    return (
        "<tr>"
        '<td style="padding:11px 0; border-bottom:1px solid #e2e8f0; '
        'font-size:13px; color:#64748b; width:34%;">'
        f"{escape(label)}</td>"
        '<td style="padding:11px 0; border-bottom:1px solid #e2e8f0; '
        'font-size:14px; color:#0f172a; font-weight:600;">'
        f"{escape(value)}</td>"
        "</tr>"
    )


def build_summary_table(alert):
    style = severity_style(alert)
    rows = [
        (
            ("Alert type", get_value(alert, "alert_type")),
            ("Severity", style["label"]),
        ),
        (
            ("Reading", reading_text(alert)),
            ("Threshold", threshold_text(alert)),
        ),
    ]

    html_rows = []
    for left, right in rows:
        html_rows.append(
            "<tr>"
            + summary_cell(left[0], left[1])
            + summary_cell(right[0], right[1])
            + "</tr>"
        )

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse; margin-top:16px;">'
        + "".join(html_rows)
        + "</table>"
    )


def build_details_table(alert):
    location = (
        f"{get_value(alert, 'location_name')} "
        f"({get_value(alert, 'location_id')})"
    )
    rows = [
        ("Sensor", get_value(alert, "sensor_id")),
        ("Location", location),
        ("Parameter", get_value(alert, "parameter")),
        ("Event time", get_value(alert, "event_time")),
        ("Processed at", get_value(alert, "processed_at")),
    ]

    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="border-collapse:collapse;">'
        + "".join(detail_row(label, value) for label, value in rows)
        + "</table>"
    )


def build_html(alert):
    style = severity_style(alert)
    alert_type = get_value(alert, "alert_type")
    location_name = get_value(alert, "location_name")
    message = get_value(alert, "message", "")

    return f"""<!doctype html>
<html>
  <body style="margin:0; padding:0; background:#f1f5f9;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; background:#f1f5f9;">
      <tr>
        <td align="center" style="padding:28px 12px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; max-width:640px; background:#ffffff; border:1px solid #e2e8f0;">
            <tr>
              <td style="padding:26px 28px; background:{style['accent']};">
                <div style="font-family:Arial, Helvetica, sans-serif; font-size:12px; color:#fff7ed; text-transform:uppercase; letter-spacing:0.08em; font-weight:700;">
                  Water Quality Monitoring
                </div>
                <h1 style="font-family:Arial, Helvetica, sans-serif; font-size:28px; line-height:34px; color:#ffffff; margin:8px 0 6px 0;">
                  Water Quality Alert
                </h1>
                <div style="font-family:Arial, Helvetica, sans-serif; font-size:15px; line-height:22px; color:#fff7ed;">
                  {escape(alert_type)} at {escape(location_name)}
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:24px 28px 10px 28px; font-family:Arial, Helvetica, sans-serif;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; background:{style['soft']}; border-left:5px solid {style['accent']};">
                  <tr>
                    <td style="padding:18px 20px;">
                      <div style="font-size:13px; color:{style['text']}; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">
                        Immediate attention recommended
                      </div>
                      <div style="font-size:16px; line-height:24px; color:#0f172a; margin-top:8px;">
                        {escape(message)}
                      </div>
                      {build_summary_table(alert)}
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:14px 28px 26px 28px; font-family:Arial, Helvetica, sans-serif;">
                <h2 style="font-size:16px; line-height:22px; color:#0f172a; margin:0 0 10px 0;">
                  Alert details
                </h2>
                {build_details_table(alert)}
              </td>
            </tr>
            <tr>
              <td style="padding:18px 28px; background:#f8fafc; border-top:1px solid #e2e8f0; font-family:Arial, Helvetica, sans-serif;">
                <div style="font-size:12px; line-height:18px; color:#64748b;">
                  Generated by Water Quality Monitoring. This notification came from the automated local Kafka and Spark pipeline.
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""
