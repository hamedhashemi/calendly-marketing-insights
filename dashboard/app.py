import pandas as pd
import plotly.express as px
import streamlit as st

from athena_client import run_query


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Calendly Marketing Insights",
    page_icon="📊",
    layout="wide"
)


st.title(
    "Calendly Marketing Insights"
)

st.caption(
    "Marketing attribution, booking behavior "
    "and employee meeting workload"
)


# ============================================================
# ATHENA DATA LOAD
# ============================================================

@st.cache_data(ttl=300)
def load_marketing():

    return run_query("""

        SELECT *

        FROM
            calendly_marketing.
            gold_daily_marketing_performance

        ORDER BY
            report_date,
            channel

    """)


@st.cache_data(ttl=300)
def load_booking_time():

    return run_query("""

        SELECT *

        FROM
            calendly_marketing.
            gold_booking_time_analysis

        ORDER BY
            report_date,
            source,
            booking_hour

    """)


@st.cache_data(ttl=300)
def load_employee_load():

    return run_query("""

        SELECT *

        FROM
            calendly_marketing.
            gold_employee_meeting_load

        ORDER BY
            week_start,
            employee_id

    """)


try:

    marketing = load_marketing()

    booking_time = load_booking_time()

    employee_load = load_employee_load()

except Exception as error:

    st.error(
        "Unable to query Athena."
    )

    st.exception(error)

    st.stop()


# ============================================================
# TYPE CONVERSION
# ============================================================

# Marketing

marketing["report_date"] = pd.to_datetime(
    marketing["report_date"]
)

for column in [

    "spend_amount",
    "all_created_bookings",
    "original_bookings",
    "rescheduled_bookings",
    "cost_per_booking"

]:

    marketing[column] = pd.to_numeric(
        marketing[column],
        errors="coerce"
    )


# Booking time

booking_time["report_date"] = pd.to_datetime(
    booking_time["report_date"]
)

for column in [

    "day_of_week_number",
    "booking_hour",
    "all_created_bookings",
    "original_bookings",
    "rescheduled_bookings",
    "booking_count"

]:

    booking_time[column] = pd.to_numeric(
        booking_time[column],
        errors="coerce"
    )


# Employee load

employee_load["week_start"] = pd.to_datetime(
    employee_load["week_start"]
)

for column in [

    "scheduled_meeting_count",
    "employee_total_meetings",
    "observed_weeks",
    "avg_meetings_per_week"

]:

    employee_load[column] = pd.to_numeric(
        employee_load[column],
        errors="coerce"
    )


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.header(
    "Dashboard Filters"
)


all_dates = pd.concat([

    marketing[
        "report_date"
    ],

    booking_time[
        "report_date"
    ]

]).dropna()


minimum_date = all_dates.min().date()

maximum_date = all_dates.max().date()


selected_dates = st.sidebar.date_input(

    "Date range",

    value=(
        minimum_date,
        maximum_date
    ),

    min_value=minimum_date,
    max_value=maximum_date

)


if isinstance(
    selected_dates,
    (list, tuple)
):

    if len(selected_dates) == 2:

        start_date = pd.Timestamp(
            selected_dates[0]
        )

        end_date = pd.Timestamp(
            selected_dates[1]
        )

    else:

        start_date = pd.Timestamp(
            selected_dates[0]
        )

        end_date = start_date

else:

    start_date = pd.Timestamp(
        selected_dates
    )

    end_date = start_date


available_sources = sorted(

    booking_time[
        "source"
    ]
    .dropna()
    .unique()
    .tolist()

)


selected_sources = st.sidebar.multiselect(

    "Booking source",

    options=available_sources,

    default=available_sources

)


st.sidebar.caption(
    "All report timestamps are normalized "
    "to America/New_York."
)


# ============================================================
# APPLY FILTERS
# ============================================================

marketing_filtered = marketing[

    (
        marketing[
            "report_date"
        ] >= start_date
    )

    &

    (
        marketing[
            "report_date"
        ] <= end_date
    )

].copy()


booking_filtered = booking_time[

    (
        booking_time[
            "report_date"
        ] >= start_date
    )

    &

    (
        booking_time[
            "report_date"
        ] <= end_date
    )

].copy()


if selected_sources:

    booking_filtered = booking_filtered[

        booking_filtered[
            "source"
        ].isin(
            selected_sources
        )

    ]


employee_filtered = employee_load[

    (
        employee_load[
            "week_start"
        ] >= start_date
    )

    &

    (
        employee_load[
            "week_start"
        ] <= end_date
    )

].copy()


# ============================================================
# TABS
# ============================================================

tab_marketing, \
tab_trends, \
tab_time, \
tab_employee = st.tabs([

    "Marketing & CPB",

    "Booking Trends",

    "Time Analysis",

    "Employee Load"

])


# ============================================================
# TAB 1
# MARKETING / CPB
# ============================================================

with tab_marketing:

    st.subheader(
        "Marketing Performance"
    )

    # --------------------------------------------------------
    # Only rows with confirmed mapping and complete
    # booking/spend coverage are valid for aggregate CPB.
    # --------------------------------------------------------

    attributable = marketing_filtered[

        (
            marketing_filtered[
                "mapping_status"
            ] == "confirmed"
        )

        &

        (
            marketing_filtered[
                "booking_data_complete"
            ].astype(str)
            .str.lower()
            == "true"
        )

        &

        (
            marketing_filtered[
                "spend_available"
            ].astype(str)
            .str.lower()
            == "true"
        )

    ].copy()


    total_spend_all = (
        marketing_filtered[
            "spend_amount"
        ]
        .sum()
    )


    attributable_spend = (
        attributable[
            "spend_amount"
        ]
        .sum()
    )


    total_bookings = (
        attributable[
            "original_bookings"
        ]
        .fillna(0)
        .sum()
    )


    blended_cpb = (

        attributable_spend
        /
        total_bookings

        if total_bookings > 0

        else None

    )


    col1, col2, col3, col4 = (
        st.columns(4)
    )


    col1.metric(
        "All-channel Spend",
        f"${total_spend_all:,.2f}"
    )


    col2.metric(
        "Attributed Spend",
        f"${attributable_spend:,.2f}"
    )


    col3.metric(
        "Original Bookings",
        f"{int(total_bookings):,}"
    )


    col4.metric(

        "Blended CPB",

        (
            f"${blended_cpb:,.2f}"
            if blended_cpb is not None
            else "N/A"
        )

    )


    pending_channels = (

        marketing_filtered[

            marketing_filtered[
                "mapping_status"
            ] != "confirmed"

        ][
            "channel"
        ]

        .dropna()

        .unique()

        .tolist()

    )


    if pending_channels:

        st.warning(

            "CPB is not calculated for channels "
            "whose Calendly mapping is still pending: "

            + ", ".join(
                pending_channels
            )

        )


    # --------------------------------------------------------
    # DAILY CPB
    # --------------------------------------------------------

    ready = marketing_filtered[

        marketing_filtered[
            "cpb_status"
        ] == "ready"

    ].copy()


    if not ready.empty:

        st.subheader(
            "Daily Cost Per Booking"
        )

        fig = px.bar(

            ready,

            x="report_date",

            y="cost_per_booking",

            color="channel",

            labels={
                "report_date": "Date",
                "cost_per_booking":
                    "Cost per Booking ($)",
                "channel":
                    "Channel"
            }

        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


    # --------------------------------------------------------
    # CHANNEL LEADERBOARD
    # --------------------------------------------------------

    st.subheader(
        "Channel Attribution"
    )


    leaderboard_rows = []


    for channel in sorted(

        marketing_filtered[
            "channel"
        ]
        .dropna()
        .unique()

    ):

        channel_df = marketing_filtered[

            marketing_filtered[
                "channel"
            ] == channel

        ]


        mapping_status = (

            channel_df[
                "mapping_status"
            ]
            .dropna()
            .iloc[0]

            if not channel_df.empty

            else "unknown"

        )


        channel_spend = (
            channel_df[
                "spend_amount"
            ].sum()
        )


        valid_channel = attributable[

            attributable[
                "channel"
            ] == channel

        ]


        channel_bookings = (
            valid_channel[
                "original_bookings"
            ]
            .fillna(0)
            .sum()
        )


        if (
            mapping_status
            == "confirmed"
            and channel_bookings > 0
        ):

            valid_spend = (
                valid_channel[
                    "spend_amount"
                ].sum()
            )

            channel_cpb = (
                valid_spend
                /
                channel_bookings
            )

        else:

            channel_cpb = None


        leaderboard_rows.append({

            "Channel": channel,

            "Mapping Status":
                mapping_status,

            "Spend":
                channel_spend,

            "Bookings":
                (
                    channel_bookings
                    if mapping_status
                    == "confirmed"
                    else None
                ),

            "CPB":
                channel_cpb

        })


    leaderboard = pd.DataFrame(
        leaderboard_rows
    )


    st.dataframe(

        leaderboard,

        use_container_width=True,

        hide_index=True,

        column_config={

            "Spend":
                st.column_config.NumberColumn(
                    format="$%.2f"
                ),

            "CPB":
                st.column_config.NumberColumn(
                    format="$%.2f"
                )

        }

    )


# ============================================================
# TAB 2
# DAILY BOOKINGS / TREND
# ============================================================

with tab_trends:

    st.subheader(
        "Daily Calls Booked by Source"
    )


    daily = (

        booking_filtered

        .groupby(
            [
                "report_date",
                "source"
            ],
            as_index=False
        )

        [
            "original_bookings"
        ]

        .sum()

        .rename(
            columns={
                "original_bookings":
                    "bookings"
            }
        )

    )


    if daily.empty:

        st.info(
            "No booking data for selected filters."
        )

    else:

        fig = px.line(

            daily,

            x="report_date",

            y="bookings",

            color="source",

            markers=True,

            labels={
                "report_date": "Date",
                "bookings": "Bookings",
                "source": "Source"
            }

        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


        st.subheader(
            "Cumulative Booking Trend"
        )


        cumulative = daily.sort_values(

            [
                "source",
                "report_date"
            ]

        ).copy()


        cumulative[
            "cumulative_bookings"
        ] = (

            cumulative

            .groupby(
                "source"
            )[
                "bookings"
            ]

            .cumsum()

        )


        fig = px.area(

            cumulative,

            x="report_date",

            y="cumulative_bookings",

            color="source",

            labels={
                "report_date": "Date",
                "cumulative_bookings":
                    "Cumulative Bookings",
                "source": "Source"
            }

        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


        st.subheader(
            "Daily Booking Table"
        )

        st.dataframe(
            daily,
            use_container_width=True,
            hide_index=True
        )


# ============================================================
# TAB 3
# BOOKING TIME ANALYSIS
# ============================================================

with tab_time:

    st.subheader(
        "Booking Volume by Time Slot / Day of Week"
    )


    if booking_filtered.empty:

        st.info(
            "No booking data for selected filters."
        )

    else:

        # ----------------------------------------------------
        # HEATMAP
        # ----------------------------------------------------

        heatmap_data = (

            booking_filtered

            .groupby(
                [
                    "day_of_week_number",
                    "day_of_week",
                    "booking_hour"
                ],
                as_index=False
            )

            [
                "booking_count"
            ]

            .sum()

        )


        pivot = (

            heatmap_data

            .pivot_table(

                index=[
                    "day_of_week_number",
                    "day_of_week"
                ],

                columns="booking_hour",

                values="booking_count",

                fill_value=0

            )

            .reset_index()

            .sort_values(
                "day_of_week_number"
            )

            .set_index(
                "day_of_week"
            )

            .drop(
                columns=[
                    "day_of_week_number"
                ]
            )

        )


        # Ensure all 24 hours exist.

        pivot = pivot.reindex(
            columns=range(24),
            fill_value=0
        )


        st.subheader(
            "Hour × Day Heatmap"
        )


        fig = px.imshow(

            pivot,

            labels={
                "x": "Hour of Day",
                "y": "Day of Week",
                "color": "Bookings"
            },

            aspect="auto"

        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


        # ----------------------------------------------------
        # BOOKINGS BY HOUR
        # ----------------------------------------------------

        hourly = (

            booking_filtered

            .groupby(
                "booking_hour",
                as_index=False
            )[
                "booking_count"
            ]

            .sum()

        )


        fig = px.bar(

            hourly,

            x="booking_hour",

            y="booking_count",

            labels={
                "booking_hour":
                    "Hour of Day",
                "booking_count":
                    "Bookings"
            }

        )


        st.subheader(
            "Bookings by Hour"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


        # ----------------------------------------------------
        # BOOKINGS BY WEEKDAY
        # ----------------------------------------------------

        weekday = (

            booking_filtered

            .groupby(
                [
                    "day_of_week_number",
                    "day_of_week"
                ],
                as_index=False
            )[
                "booking_count"
            ]

            .sum()

            .sort_values(
                "day_of_week_number"
            )

        )


        fig = px.pie(

            weekday,

            names="day_of_week",

            values="booking_count",

            hole=0.35

        )


        st.subheader(
            "Bookings by Day of Week"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


# ============================================================
# TAB 4
# EMPLOYEE MEETING LOAD
# ============================================================

with tab_employee:

    st.subheader(
        "Employee Meeting Load"
    )


    if employee_filtered.empty:

        st.info(
            "No employee workload data "
            "for selected date range."
        )

    else:

        # ----------------------------------------------------
        # KPIs
        # ----------------------------------------------------

        total_employee_meetings = (

            employee_filtered[
                "scheduled_meeting_count"
            ]
            .sum()

        )


        max_weekly = (

            employee_filtered[
                "scheduled_meeting_count"
            ]
            .max()

        )


        min_weekly = (

            employee_filtered[
                "scheduled_meeting_count"
            ]
            .min()

        )


        c1, c2, c3 = (
            st.columns(3)
        )


        c1.metric(
            "Scheduled Employee-Meetings",
            f"{int(total_employee_meetings):,}"
        )


        c2.metric(
            "Maximum Weekly Load",
            f"{int(max_weekly):,}"
        )


        c3.metric(
            "Minimum Weekly Load",
            f"{int(min_weekly):,}"
        )


        # ----------------------------------------------------
        # EMPLOYEE SUMMARY
        #
        # employee_total_meetings and avg are repeated
        # once per week, so deduplicate by employee.
        # ----------------------------------------------------

        employee_summary = (

            employee_filtered[

                [
                    "employee_id",
                    "employee_total_meetings",
                    "observed_weeks",
                    "avg_meetings_per_week"
                ]

            ]

            .drop_duplicates(
                subset=[
                    "employee_id"
                ]
            )

            .sort_values(
                "employee_total_meetings",
                ascending=False
            )

        )


        st.subheader(
            "Average Meetings per Week"
        )


        fig = px.bar(

            employee_summary,

            x="avg_meetings_per_week",

            y="employee_id",

            orientation="h",

            hover_data=[
                "employee_total_meetings",
                "observed_weeks"
            ],

            labels={
                "employee_id":
                    "Employee",
                "avg_meetings_per_week":
                    "Average Meetings / Week"
            }

        )


        fig.update_layout(
            yaxis={
                "categoryorder":
                    "total ascending"
            }
        )


        st.plotly_chart(
            fig,
            use_container_width=True
        )


        # ----------------------------------------------------
        # WEEKLY TREND
        # ----------------------------------------------------

        st.subheader(
            "Weekly Employee Trend"
        )


        employees = (

            employee_filtered[
                "employee_id"
            ]
            .dropna()
            .unique()
            .tolist()

        )


        selected_employee = (
            st.selectbox(
                "Employee",
                sorted(employees)
            )
        )


        employee_weekly = (

            employee_filtered[

                employee_filtered[
                    "employee_id"
                ]
                == selected_employee

            ]

            .sort_values(
                "week_start"
            )

        )


        fig = px.line(

            employee_weekly,

            x="week_start",

            y="scheduled_meeting_count",

            markers=True,

            labels={
                "week_start":
                    "Week",
                "scheduled_meeting_count":
                    "Scheduled Meetings"
            }

        )


        st.plotly_chart(
            fig,
            use_container_width=True
        )


        st.dataframe(

            employee_summary,

            use_container_width=True,

            hide_index=True

        )


# ============================================================
# DATA QUALITY / MODEL NOTES
# ============================================================

st.divider()

with st.expander(
    "Data interpretation notes"
):

    st.markdown(
        """
        - **Original bookings** are used as new acquisitions;
          reschedules are not counted as new bookings.
        - CPB is calculated only where the Calendly-to-channel
          mapping is confirmed and both booking and spend data
          are complete.
        - YouTube and TikTok remain pending until their active
          Calendly mappings are confirmed.
        - Booking time analytics use Eastern Time
          (`America/New_York`).
        - Employee workload represents **scheduled meetings**,
          not verified attendance.
        """
    )