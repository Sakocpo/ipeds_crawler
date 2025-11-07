from typing import Any, List
import os
import traceback
import pandas as pd
from rich import print

from .browser import browser_page
from .ipeds_pages import goto_reported_data
from .extractors import wait_for_all, get_text_data, get_box_data
from .normalize import build_labeled_dict, normalize


async def run_pipeline(input_df: pd.DataFrame, output_path: str, min_year: int = 2014, max_year: int = 2023) -> None:
    name_list = input_df["INSTNM"].tolist()
    id_list = input_df["UNITID"].tolist()

    async with browser_page() as page:
        for name, unit_id in zip(name_list, id_list):
            for year in range(max_year, min_year - 1, -1):
                try:
                    print(f"[bold]{name}[/bold] Year: {year}")

                    # ---------------------------
                    # INSTITUTIONAL PAGE (Survey 1) — Pricing
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 1, year)
                    if year == 2023:
                        table_found = await wait_for_all(frame, "table.sc.survey-t")
                    else:
                        table_found = await wait_for_all(frame, "table.grid")

                    if table_found:
                        tuition_fee = await get_text_data(frame, "out-of-state tuition and fees", year)
                        if not tuition_fee:
                            tuition_fee = await get_text_data(
                                frame,
                                "Published Tuition and fees" if year == 2023 else "Tuition and fees",
                                year,
                            )
                        if tuition_fee:
                            tuition_fee = tuition_fee[-1] if len(tuition_fee) == 4 else tuition_fee[3]

                        book_and_supplies = await get_text_data(frame, "Books and supplies", year)
                        book_and_supplies = (
                            book_and_supplies[-1] if book_and_supplies else book_and_supplies
                        )

                        food_housing_on_campus = await get_text_data(
                            frame,
                            "On-campus food and housing" if year == 2023 else "On-campus room and board",
                            year,
                        )
                        food_housing_on_campus = (
                            food_housing_on_campus[-1]
                            if food_housing_on_campus
                            else food_housing_on_campus
                        )

                        other_expenses_on_campus = await get_text_data(
                            frame, "On-campus other expenses", year
                        )
                        other_expenses_on_campus = (
                            other_expenses_on_campus[-1]
                            if other_expenses_on_campus
                            else other_expenses_on_campus
                        )

                        food_housing_off_campus = await get_text_data(
                            frame,
                            "Off-campus food and housing" if year == 2023 else "Off-campus room and board",
                            year,
                        )
                        food_housing_off_campus = (
                            food_housing_off_campus[-1]
                            if food_housing_off_campus
                            else food_housing_off_campus
                        )

                        other_expenses_off_campus = await get_text_data(
                            frame, "Off-campus other expenses", year
                        )
                        other_expenses_off_campus = (
                            other_expenses_off_campus[-1]
                            if other_expenses_off_campus
                            else other_expenses_off_campus
                        )

                        other_expenses_off_campus_family = await get_text_data(
                            frame, "Off-campus with family other expenses", year
                        )
                        other_expenses_off_campus_family = (
                            other_expenses_off_campus_family[-1]
                            if other_expenses_off_campus_family
                            else other_expenses_off_campus_family
                        )
                    else:
                        print(f"[yellow][WARN][/yellow] pricing information table not found for {year}")
                        tuition_fee = None
                        book_and_supplies = None
                        food_housing_on_campus = None
                        other_expenses_on_campus = None
                        food_housing_off_campus = None
                        other_expenses_off_campus = None
                        other_expenses_off_campus_family = None

                    pricing_data = build_labeled_dict(
                        ("tuition_fee", "", tuition_fee, None),
                        ("book_and_supplies", "", book_and_supplies, None),
                        ("food_housing_on_campus", "", food_housing_on_campus, None),
                        ("other_expenses_on_campus", "", other_expenses_on_campus, None),
                        ("food_housing_off_campus", "", food_housing_off_campus, None),
                        ("other_expenses_off_campus", "", other_expenses_off_campus, None),
                        ("other_expenses_off_campus_family", "", other_expenses_off_campus_family, None),
                    )

                    # ---------------------------
                    # ADMISSIONS & TEST SCORE (Survey 12)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 12, year)
                    if year == 2023:
                        table_selector = "table.sc.survey-t"
                        value_selector = "td.sc-tb-r.t-co span"
                    elif 2020 <= year <= 2022:
                        table_selector = "table.grid"
                        value_selector = "td.number"
                    else:
                        table_selector = "table.sc"
                        value_selector = "span"

                    table_found = await wait_for_all(frame, table_selector)
                    if table_found:
                        num_applicant = await get_text_data(frame, "Number of applicants", year)
                        percent_admitted = await get_text_data(frame, "Percent admitted", year)
                        percent_admitted_enrolled = await get_text_data(
                            frame, "Percent admitted who enrolled", year
                        )
                        sat = await get_text_data(
                            frame, "SAT", year, table_selector=table_selector, value_selector=value_selector
                        )
                        act = await get_text_data(
                            frame, "ACT", year, table_selector=table_selector, value_selector=value_selector
                        )

                        if year == 2023:
                            if sat:
                                sat.pop(3)
                                sat.pop(6)
                            if act:
                                act.pop(3)
                                act.pop(6)
                    else:
                        print(f"[yellow][WARN][/yellow] Admission & test score table not found for {year}")
                        num_applicant = []
                        percent_admitted = []
                        percent_admitted_enrolled = []
                        sat = []
                        act = []

                    col1 = ["total", "male", "female"]
                    col2 = ["num_submitted", "pct_submitted"]
                    col3 = ["25th_pct", "75th_pct"]

                    admission_data = build_labeled_dict(
                        ("num_applicant", col1, num_applicant, slice(None)),
                        ("percent_admitted", col1, percent_admitted, slice(None)),
                        ("percent_admitted_enrolled", col1, percent_admitted_enrolled, slice(None)),
                        ("sat", col2, sat, slice(0, 2)),
                        ("act", col2, act, slice(0, 2)),
                        ("sat_rw", col3, sat, slice(2, 4)),
                        ("sat_math", col3, sat, slice(4, 6)),
                        ("act_comp", col3, act, slice(2, 4)),
                        ("act_eng", col3, act, slice(4, 6)),
                        ("act_math", col3, act, slice(6, 8)),
                    )

                    # ---------------------------
                    # ENROLLMENT (Survey 15)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 15, year)
                    table_selector = "table.sc.survey-t" if year == 2023 else "table.grid"
                    table_found = await wait_for_all(frame, table_selector)

                    if table_found:
                        total_enrollment = await get_text_data(frame, "Total enrollment", year)
                        both_enrol = await get_text_data(frame, "Graduate enrollment", year)
                        if isinstance(both_enrol, list) and both_enrol:
                            undergrad_enrollment, grad_enrollment = both_enrol
                        else:
                            undergrad_enrollment = both_enrol
                            grad_enrollment = None

                        female_percentage = await get_text_data(
                            frame, "Percent of all students who are female", year
                        )
                        female_percentage = (
                            female_percentage[1]
                            if isinstance(female_percentage, list) and female_percentage
                            else female_percentage
                        )
                        if year == 2023:
                            temp_text = "U .S. Nonresident (%)"
                        elif 2021 < year <= 2022:
                            temp_text = "U.S. Nonresident"
                        else:
                            temp_text = "Nonresident alien"
                        international_student_percent = await get_text_data(frame, temp_text, year)
                        if not international_student_percent:
                            international_student_percent = None
                        elif isinstance(international_student_percent, list):
                            international_student_percent = international_student_percent[1]
                    else:
                        print("[yellow][WARN][/yellow] Enrollment table not found")
                        total_enrollment = None
                        undergrad_enrollment = None
                        grad_enrollment = None
                        female_percentage = None
                        international_student_percent = None

                    enrol_data = build_labeled_dict(
                        ("total_enrollment", "", total_enrollment, None),
                        ("undergrad_enrollment", "", undergrad_enrollment, None),
                        ("grad_enrollment", "", grad_enrollment, None),
                        ("female_percentage", "", female_percentage, None),
                        ("international_student_percent", "", international_student_percent, None),
                    )

                    # ---------------------------
                    # COMPLETIONS (Survey 3)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 3, year)
                    if year > 2019:
                        table_selector = "table.table.table-bordered.sc"
                        value_selector = "td.number"
                    else:
                        table_selector = "table.sc"
                        value_selector = "td.sc-tb-c"

                    table_found = await wait_for_all(frame, table_selector)
                    if table_found:
                        Bs = await get_text_data(
                            frame, "Bachelor's degree", year, table_selector=table_selector, value_selector=value_selector
                        )
                        if not Bs:
                            Bs = await get_text_data(
                                frame, "Bachelors degree", year, table_selector=table_selector, value_selector=value_selector
                            )
                        Ms = await get_text_data(
                            frame, "Master's degree", year, table_selector=table_selector, value_selector=value_selector
                        )
                        Phd = await get_text_data(
                            frame, "Doctor's degree - research", year, table_selector=table_selector, value_selector=value_selector
                        )
                        total_completors = await get_text_data(
                            frame, "All Completers", year, table_selector=table_selector, value_selector=value_selector
                        )
                    else:
                        print(f"[yellow][WARN][/yellow] completions table not found for {year}")
                        Bs = []
                        Ms = []
                        Phd = []
                        total_completors = []

                    completions_data = build_labeled_dict(
                        ("Bs", ["1st_major", "2nd_major"], Bs, None, "first"),
                        ("Ms", ["1st_major", "2nd_major"], Ms, None, "first"),
                        ("Phd", ["1st_major", "2nd_major"], Phd, None, "first"),
                        ("total_completors", ["male", "female", ""], total_completors, None, "last"),
                    )

                    # ---------------------------
                    # GRADUATION (Survey 8)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 8, year)
                    if year == 2023:
                        table_found = await wait_for_all(frame, "table.sc.survey-t")
                    else:
                        table_found = await wait_for_all(frame, "table.grid")

                    if table_found:
                        graduation_rate_pct = await get_text_data(
                            frame, "Graduation rate", year, table_header="Overall Graduation Rate"
                        )
                        graduation_rate_pct = (
                            graduation_rate_pct[0]
                            if isinstance(graduation_rate_pct, list)
                            else graduation_rate_pct
                        )

                        total_graduated = await get_text_data(
                            frame, "Total number of students in the Adjusted Cohort", year
                        )
                        total_graduated = (
                            total_graduated[0] if isinstance(total_graduated, list) else total_graduated
                        )

                        total_graduated_150_time = await get_text_data(
                            frame, "Total number of completers within 150", year
                        )
                        total_graduated_150_time = (
                            total_graduated_150_time[0]
                            if isinstance(total_graduated_150_time, list)
                            else total_graduated_150_time
                        )
                    else:
                        print(f"[yellow][WARN][/yellow] Graduation table not found for {year}")
                        graduation_rate_pct = []
                        total_graduated = []
                        total_graduated_150_time = []

                    graduation_data = build_labeled_dict(
                        ("graduation_rate_pct", "", graduation_rate_pct, None),
                        ("total_graduated", "", total_graduated, None),
                        ("total_graduated_150_time", "", total_graduated_150_time, None),
                    )

                    # ---------------------------
                    # STUDENT FINANCIAL AID (Survey 7)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 7, year)
                    if year == 2023:
                        table_selector = "table.sc.survey-t"
                    else:
                        table_selector = "table.sc.survey-t" if 2020 <= year < 2023 else "table.sc"

                    table_found = await wait_for_all(frame, table_selector)
                    if table_found:
                        if year > 2019:
                            func = get_box_data
                        else:
                            func = get_text_data

                        temp_text = (
                            "Grant or scholarship aid from the federal government, state/local government, "
                            "the institution, and other sources known to the institution"
                        )
                        all_grant_scholarship = await func(
                            frame,
                            temp_text,
                            year,
                            table_selector=table_selector,
                            value_selector="td.sc-tb-r.t-c-t input.sc-tbn"
                            if year >= 2022
                            else ("td.sc-tb-r.t-co input.sc-tbn" if 2020 <= year < 2022 else "td.sc-tb-l span"),
                        )
                        num_awarded_aid, total_amount_awarded_aid = (
                            all_grant_scholarship if isinstance(all_grant_scholarship, list) else [None, None]
                        )

                        all_grant_scholarship_txt = await get_text_data(
                            frame,
                            temp_text,
                            year,
                            table_selector=table_selector,
                            value_selector="td.sc-tb-r.t-c-t span"
                            if year >= 2022
                            else ("td.sc-tb-r.t-co span" if 2020 <= year < 2022 else "td.sc-s.sc-tb-l"),
                        )
                        all_grant_scholarship_txt = (
                            all_grant_scholarship_txt[0:2]
                            if isinstance(all_grant_scholarship_txt, list)
                            else [None, None]
                        )
                        pct_awarded_aid, avg_amount_awarded_aid = all_grant_scholarship_txt

                        federal_pell_grant_box = await func(
                            frame,
                            "Pell Grants",
                            year,
                            table_selector=table_selector,
                            value_selector="td.sc-tb-r.t-c-t input.sc-tbn"
                            if year >= 2022
                            else ("td.sc-tb-r.t-co input.sc-tbn" if 2020 <= year < 2022 else "td.sc-tb-l span"),
                        )
                        num_awarded_pell_grant, total_amount_awarded_pell_grant = (
                            federal_pell_grant_box if isinstance(federal_pell_grant_box, list) else [None, None]
                        )

                        federal_pell_grant_txt = await get_text_data(
                            frame,
                            "Pell Grants",
                            year,
                            table_selector=table_selector,
                            value_selector="td.sc-tb-r.t-c-t span"
                            if year >= 2022
                            else ("td.sc-tb-r.t-co span" if 2020 <= year < 2022 else "td.sc-s.sc-tb-l"),
                        )
                        federal_pell_grant_txt = (
                            federal_pell_grant_txt[0:2]
                            if isinstance(federal_pell_grant_txt, list)
                            else [None, None]
                        )
                        pct_awarded_pell_grant, avg_amount_awarded_pell_grant = federal_pell_grant_txt
                    else:
                        print(f"[yellow][WARN][/yellow] Financial aid table not found for {year}")
                        (
                            num_awarded_aid,
                            total_amount_awarded_aid,
                            pct_awarded_aid,
                            avg_amount_awarded_aid,
                            num_awarded_pell_grant,
                            total_amount_awarded_pell_grant,
                            pct_awarded_pell_grant,
                            avg_amount_awarded_pell_grant,
                        ) = (None, None, None, None, None, None, None, None)

                    financial_aid_data = {
                        "num_awarded_aid": num_awarded_aid,
                        "total_amount_awarded_aid": total_amount_awarded_aid,
                        "pct_awarded_aid": pct_awarded_aid,
                        "avg_amount_awarded_aid": avg_amount_awarded_aid,
                        "num_awarded_pell_grant": num_awarded_pell_grant,
                        "total_amount_awarded_pell_grant": total_amount_awarded_pell_grant,
                        "pct_awarded_pell_grant": pct_awarded_pell_grant,
                        "avg_amount_awarded_pell_grant": avg_amount_awarded_pell_grant,
                    }

                    # ---------------------------
                    # FINANCE (Survey 6)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 6, year)
                    table_selector = "table.sc.survey-t" if year == 2023 else "table.grid"
                    table_found = await wait_for_all(frame, table_selector)

                    if table_found:
                        # sequentially (no concurrency) to stay close to your script
                        tuition_revenue_per_fte = await get_text_data(frame, "Tuition and fees", year)
                        gov_grants_revenue_per_fte = await get_text_data(
                            frame, "Government grants and contracts", year
                        )
                        private_revenue_per_fte = await get_text_data(
                            frame, "Private gifts, grants, and contracts", year
                        )
                        total_core_revenue_per_fte = await get_text_data(
                            frame, "Total core revenues", year
                        )
                        instruction_expense_per_fte = await get_text_data(frame, "Instruction", year)
                        academic_support_expense_per_fte = await get_text_data(
                            frame, "Academic support", year
                        )
                        student_services_expense_per_fte = await get_text_data(
                            frame, "Student services", year
                        )
                        total_core_expense_per_fte = await get_text_data(
                            frame, "Total core expenses", year
                        )
                        num_fte_enrollment = await get_text_data(frame, "FTE enrollment", year)

                        def take_last(x):
                            if isinstance(x, list) and x:
                                return x[-1]
                            return x

                        tuition_revenue_per_fte = take_last(tuition_revenue_per_fte)
                        gov_grants_revenue_per_fte = take_last(gov_grants_revenue_per_fte)
                        private_revenue_per_fte = take_last(private_revenue_per_fte)
                        total_core_revenue_per_fte = take_last(total_core_revenue_per_fte)
                        instruction_expense_per_fte = take_last(instruction_expense_per_fte)
                        academic_support_expense_per_fte = take_last(academic_support_expense_per_fte)
                        student_services_expense_per_fte = take_last(student_services_expense_per_fte)
                        total_core_expense_per_fte = take_last(total_core_expense_per_fte)
                        num_fte_enrollment = take_last(num_fte_enrollment)
                    else:
                        print(f"[yellow][WARN][/yellow] finance table not found for {year}")
                        tuition_revenue_per_fte = None
                        gov_grants_revenue_per_fte = None
                        private_revenue_per_fte = None
                        total_core_revenue_per_fte = None
                        instruction_expense_per_fte = None
                        academic_support_expense_per_fte = None
                        student_services_expense_per_fte = None
                        total_core_expense_per_fte = None
                        num_fte_enrollment = None

                    financial_data = {
                        "tuition_revenue_per_fte": tuition_revenue_per_fte,
                        "gov_grants_revenue_per_fte": gov_grants_revenue_per_fte,
                        "private_revenue_per_fte": private_revenue_per_fte,
                        "total_core_revenue_per_fte": total_core_revenue_per_fte,
                        "instruction_expense_per_fte": instruction_expense_per_fte,
                        "academic_support_expense_per_fte": academic_support_expense_per_fte,
                        "student_services_expense_per_fte": student_services_expense_per_fte,
                        "total_core_expense_per_fte": total_core_expense_per_fte,
                        "num_fte_enrollment": num_fte_enrollment,
                    }

                    # ---------------------------
                    # HUMAN RESOURCE (Survey 9)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 9, year)
                    table_selector = "table.sc.survey-t" if year == 2023 else "table.grid"
                    table_found = await wait_for_all(frame, table_selector)

                    if table_found:
                        instruct_staff = await get_text_data(frame, "Instructional Staff", year)
                        instruct_staff = instruct_staff[-1] if instruct_staff else instruct_staff

                        academic_affairs = await get_text_data(
                            frame, "Library and Student and Academic Affairs and Other Education Services Occupations SOC", year
                        )
                        academic_affairs = academic_affairs[-1] if academic_affairs else academic_affairs

                        it_occupation = await get_text_data(
                            frame, "Computer, Engineering, and Science Occupations", year
                        )
                        it_occupation = it_occupation[-1] if len(it_occupation) == 3 else it_occupation[-3]

                        management_occupation = await get_text_data(frame, "Management Occupations", year)
                        if management_occupation:
                            management_occupation = (
                                management_occupation[-1]
                                if len(management_occupation) == 3
                                else management_occupation[-3]
                            )
                    else:
                        print(f"[yellow][WARN][/yellow] Human resource page not found for {year}")
                        instruct_staff = []
                        academic_affairs = []
                        it_occupation = []
                        management_occupation = []

                    human_resource_data = build_labeled_dict(
                        ("instructional", "num_fte", instruct_staff, None, "first"),
                        ("academic_affairs", "num_fte", academic_affairs, None, "first"),
                        ("it_occupation", "num_fte", it_occupation, None, "first"),
                        ("management_occupation", "num_fte", management_occupation, None, "first"),
                    )

                    # ---------------------------
                    # ACADEMIC LIBRARY (Survey 16)
                    # ---------------------------
                    frame = await goto_reported_data(page, unit_id, 16, year)
                    table_selector = "table.sc.survey-t" if year == 2023 else "table.grid"
                    table_found = await wait_for_all(frame, table_selector)

                    if table_found:
                        library_circulation = await get_text_data(frame, "Circulation", year)
                        if library_circulation and isinstance(library_circulation, list):
                            physical_item_circulation, digital_item_circulation = library_circulation
                        elif isinstance(library_circulation, int):
                            physical_item_circulation, digital_item_circulation = None, library_circulation
                        else:
                            physical_item_circulation, digital_item_circulation = None, None
                    else:
                        print(f"[yellow][WARN][/yellow] Library page not found for {year}")
                        physical_item_circulation, digital_item_circulation = None, None

                    library_data = {
                        "physical_item_circulation": physical_item_circulation,
                        "digital_item_circulation": digital_item_circulation,
                    }

                    # ---------------------------
                    # Merge & write one record
                    # ---------------------------
                    merged_dict: dict[str, Any] = {
                        **pricing_data,
                        **admission_data,
                        **enrol_data,
                        **completions_data,
                        **graduation_data,
                        **financial_aid_data,
                        **financial_data,
                        **human_resource_data,
                        **library_data,
                    }
                    merged_dict["year"] = year
                    merged_dict["institution"] = name

                    pd.DataFrame([merged_dict]).to_csv(
                        output_path, mode="a", header=not os.path.exists(output_path), index=False
                    )

                except Exception as e:
                    print(f"[red][ERROR][/red] {name} {year}: {e}")
                    traceback.print_exc()
