import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import win32com.client as win32  # ✅ Excel COM 사용

# =========================================================
# 경로 및 기본 설정
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

TRASS_DB_PATH = BASE_DIR / "trass_exports.db"    # 잠정치 DB
ITEM_DB_PATH = BASE_DIR / "item_exports.db"     # 월별 확정/전국 DB
WORKLIST_PATH = BASE_DIR / "작업리스트.xlsx"    # 작업리스트 파일

TRASS_TABLE = "provisional_exports"             # trass DB 테이블명
ITEM_TABLE = "item_exports"                     # item_exports DB 테이블명

# 36개월 윈도우
WINDOW_MONTHS = 36

# ✅ 기준일을 코드에서 직접 입력
REF_DATE_STR = "2025.11.10"   # 예: "2025.11.10", "2025-11-10", "20251110"


# =========================================================
# 날짜/월 관련 유틸
# =========================================================

def parse_ref_date(s: str) -> datetime:
    """
    '2025.11.10', '2025-11-10', '20251110' 형식을 받아 datetime으로 변환
    """
    s = s.strip()
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"지원하지 않는 날짜 형식입니다: {s} (예: 2025.11.10)")


def infer_snap_from_day(day: int) -> str:
    """
    일(day)에 따라 snap 문자열 결정
    1~10  -> '10'
    11~20 -> '20'
    21~31 -> '30'
    """
    if day <= 10:
        return "10"
    elif day <= 20:
        return "20"
    else:
        return "30"


def add_months(yyyymm: str, months: int) -> str:
    """
    'YYYYMM' 문자열에 months 개월 더하기/빼기
    예: add_months('202401', 11) -> '202412'
    """
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])

    total_month = year * 12 + (month - 1) + months
    new_year = total_month // 12
    new_month = total_month % 12 + 1

    return f"{new_year:04d}{new_month:02d}"


def yyyymm_to_period(yyyymm: str) -> str:
    """
    'YYYYMM' -> 'YY.MM' (예: 202512 -> '25.12')
    """
    year = yyyymm[:4]
    month = yyyymm[4:6]
    return f"{year[2:]}.{month}"


def fmt_signed(value, ndigits: int = 1) -> str:
    """
    +58.3, -4.5, 0.0 형태로 문자열 포맷
    value가 None이면 빈 문자열
    """
    if value is None:
        return ""
    v = round(float(value), ndigits)
    if v > 0:
        return f"+{v:.{ndigits}f}"
    elif v < 0:
        return f"{v:.{ndigits}f}"  # 음수는 - 포함
    else:
        return f"{0:.{ndigits}f}"


# =========================================================
# DB 유틸
# =========================================================

def get_connection(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
    return sqlite3.connect(db_path)


def get_provisional_agg(
    conn: sqlite3.Connection,
    yyyymm: str,
    snap: str,
    hs_codes: list[str],
) -> tuple[float, float, int, int]:
    """
    trass_exports.db (provisional_exports)에서
    지정 yyyymm, snap, hs_codes 전체에 대한 수출금액/중량 합계와
    totaldays, workdays를 읽어온다.

    totaldays, workdays는 HS 코드별로 모두 동일하다고 가정하고,
    MIN/MAX가 다르면 예외를 발생시킨다.
    """
    if not hs_codes:
        raise ValueError("HS 코드 리스트가 비어 있습니다.")

    placeholders = ",".join(["?"] * len(hs_codes))
    sql = f"""
        SELECT
            SUM(usd_raw)      AS usd_sum,
            SUM(kg_raw)       AS kg_sum,
            MIN(totaldays)    AS min_totaldays,
            MAX(totaldays)    AS max_totaldays,
            MIN(workdays)     AS min_workdays,
            MAX(workdays)     AS max_workdays
        FROM {TRASS_TABLE}
        WHERE yyyymm = ?
          AND snap   = ?
          AND hs_code IN ({placeholders})
    """
    params = [yyyymm, snap] + hs_codes
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()

    if not row or row[0] is None:
        raise RuntimeError(
            f"[trass_exports] yyyymm={yyyymm}, snap={snap}, hs={hs_codes} 에 대한 데이터가 없습니다."
        )

    usd_sum = float(row[0]) if row[0] is not None else 0.0
    kg_sum = float(row[1]) if row[1] is not None else 0.0

    min_totaldays, max_totaldays = row[2], row[3]
    min_workdays, max_workdays = row[4], row[5]

    if min_totaldays is None or min_workdays is None:
        raise RuntimeError(
            f"[trass_exports] yyyymm={yyyymm}, snap={snap}, hs={hs_codes} 의 totaldays/workdays가 NULL 입니다."
        )

    if min_totaldays != max_totaldays or min_workdays != max_workdays:
        raise RuntimeError(
            f"[trass_exports] yyyymm={yyyymm}, snap={snap}, hs={hs_codes} 에서 "
            f"totaldays/workdays 값이 HS 코드별로 일관되지 않습니다. "
            f"(totaldays: {min_totaldays}~{max_totaldays}, workdays: {min_workdays}~{max_workdays})"
        )

    totaldays = int(max_totaldays)
    workdays = int(max_workdays)

    return usd_sum, kg_sum, totaldays, workdays


def get_item_monthly_series(
    conn: sqlite3.Connection,
    hs_codes: list[str],
) -> pd.DataFrame:
    """
    item_exports.db (item_exports)에서
    주어진 hs_codes에 대한 전체 월별 합계 시계열을 반환
    컬럼: yyyymm, usd, kg
    """
    if not hs_codes:
        raise ValueError("HS 코드 리스트가 비어 있습니다.")

    placeholders = ",".join(["?"] * len(hs_codes))
    sql = f"""
        SELECT
            yyyymm,
            SUM(usd_raw) AS usd_sum,
            SUM(kg_raw)  AS kg_sum
        FROM {ITEM_TABLE}
        WHERE hs_code IN ({placeholders})
        GROUP BY yyyymm
        ORDER BY yyyymm
    """
    cur = conn.cursor()
    cur.execute(sql, hs_codes)
    rows = cur.fetchall()

    if not rows:
        raise RuntimeError(
            f"[item_exports] hs={hs_codes} 에 대한 월별 데이터가 없습니다."
        )

    df = pd.DataFrame(rows, columns=["yyyymm", "usd", "kg"])
    return df


# =========================================================
# 36개월 윈도우 + 연속성 체크
# =========================================================

def build_36m_window(
    full_df: pd.DataFrame,
    target_yyyymm: str,
) -> pd.DataFrame:
    """
    full_df: yyyymm ASC, 컬럼: yyyymm, usd, kg
    target_yyyymm: 기준 월 (예: '202511')

    → '기준월 직전까지 35개월'만 반환.
       (이후 process_job에서 기준월(추정치)을 추가해서 36개월 완성)

    조건:
      - item_exports의 마지막 월은 반드시 기준월 직전(prev_yyyymm)이어야 함
      - prev_yyyymm 포함 과거 35개월이 연속(월단위)이어야 함
    """
    prev_yyyymm = add_months(target_yyyymm, -1)

    full_months = list(full_df["yyyymm"])
    if not full_months:
        raise RuntimeError("[연속성 오류] item_exports에 월별 데이터가 없습니다.")

    # 마지막 월 = 기준월 직전 강제
    if full_months[-1] != prev_yyyymm:
        raise RuntimeError(
            f"[연속성 오류] item_exports의 마지막 월은 기준월 직전({prev_yyyymm})이어야 합니다. "
            f"현재 마지막 월: {full_months[-1]}"
        )

    if prev_yyyymm not in full_months:
        raise RuntimeError(
            f"[연속성 오류] 기준월 직전({prev_yyyymm})이 item_exports에 존재하지 않습니다."
        )

    idx_prev = full_months.index(prev_yyyymm)

    needed_months = WINDOW_MONTHS - 1  # 기준월 제외 35개월

    if idx_prev < (needed_months - 1):
        raise RuntimeError(
            f"[연속성 오류] 기준월 직전까지 최소 {needed_months}개월 데이터가 필요합니다. "
            f"현재 보유: {idx_prev + 1}개월"
        )

    # 35개월 슬라이스
    idx_start = idx_prev - (needed_months - 1)
    window_months = full_months[idx_start: idx_prev + 1]  # 정확히 35개월

    # 연속성 체크
    for i in range(len(window_months) - 1):
        expected = add_months(window_months[i], 1)
        if window_months[i + 1] != expected:
            raise RuntimeError(
                f"[연속성 오류] {window_months[i]} 이후에 {expected}가 와야 하는데 "
                f"{window_months[i+1]} 이(가) 존재합니다."
            )

    df_35 = full_df[full_df["yyyymm"].isin(window_months)].copy()
    df_35 = df_35.sort_values("yyyymm").reset_index(drop=True)

    if len(df_35) != needed_months:
        raise RuntimeError(
            f"[연속성 오류] 35개월 윈도우를 기대했으나, 실제 행 수는 {len(df_35)} 입니다."
        )

    return df_35


# =========================================================
# 36개월 테이블 계산 (금액/중량/단가 + YoY/MoM/12M)
# =========================================================

def build_metrics_table(
    full_df: pd.DataFrame,
    df_36: pd.DataFrame,
) -> list[list]:
    """
    full_df: item_exports 전체 히스토리 (yyyymm, usd, kg)
    df_36 : 기준월 추정치를 포함한 36개월 (yyyymm, usd, kg), asc

    리턴: [
      [기간, 수출금액, YoY, MoM, 누적12M(수출금액), 누적12M YoY, 누적12M MoM,
       수출중량, 수출중량 YoY, 수출단가, 수출단가 YoY],
      ...
    ] (36행)
    """

    # 1) 히스토리(확정치) 기반 dict
    usd_hist = dict(zip(full_df["yyyymm"], full_df["usd"]))
    kg_hist  = dict(zip(full_df["yyyymm"], full_df["kg"]))

    # 2) 히스토리 + 36개월(추정치 포함)을 합친 dict (← 여기 기준으로 12M 계산)
    usd_all = usd_hist.copy()
    kg_all  = kg_hist.copy()
    for row in df_36.itertuples(index=False):
        yyyymm = getattr(row, "yyyymm")
        usd    = getattr(row, "usd")
        kg     = getattr(row, "kg")
        usd_all[yyyymm] = usd
        kg_all[yyyymm]  = kg

    # 3) 누적 12M(수출금액) 계산 (추정 월 포함 전체 시계열 기준)
    cum12_usd = {}
    all_months_all = sorted(usd_all.keys())

    for yyyymm in all_months_all:
        start_12 = add_months(yyyymm, -11)
        end_12   = yyyymm

        if start_12 < all_months_all[0]:
            continue

        months_12 = []
        cur = start_12
        while cur <= end_12:
            months_12.append(cur)
            cur = add_months(cur, 1)

        if len(months_12) != 12:
            continue

        ok = True
        vals = []
        for m in months_12:
            v = usd_all.get(m)
            if v is None:
                ok = False
                break
            vals.append(v)

        if not ok:
            continue

        cum12_usd[yyyymm] = sum(vals)

    # 4) 36개월 테이블 만들기
    rows = []
    months_36 = list(df_36["yyyymm"])
    usd_36 = list(df_36["usd"])
    kg_36  = list(df_36["kg"])

    for idx, yyyymm in enumerate(months_36):
        usd = usd_36[idx]
        kg  = kg_36[idx]

        period_str = yyyymm_to_period(yyyymm)

        # ---------- YoY / MoM (월별 금액) ----------
        usd_mom = None
        if idx > 0:
            prev_usd = usd_36[idx - 1]
            if prev_usd is not None and prev_usd != 0 and usd is not None:
                usd_mom = (usd / prev_usd - 1.0) * 100.0

        usd_yoy = None
        yyyymm_prev_year = add_months(yyyymm, -12)
        prev_year_usd = usd_all.get(yyyymm_prev_year)
        if prev_year_usd is not None and prev_year_usd != 0 and usd is not None:
            usd_yoy = (usd / prev_year_usd - 1.0) * 100.0

        # ---------- 누적12M (금액) ----------
        c12 = cum12_usd.get(yyyymm)
        c12_mom = None
        c12_yoy = None

        yyyymm_prev = add_months(yyyymm, -1)
        if c12 is not None and yyyymm_prev in cum12_usd:
            prev_c12 = cum12_usd[yyyymm_prev]
            if prev_c12 is not None and prev_c12 != 0:
                c12_mom = (c12 / prev_c12 - 1.0) * 100.0

        if c12 is not None and yyyymm_prev_year in cum12_usd:
            prev_year_c12 = cum12_usd[yyyymm_prev_year]
            if prev_year_c12 is not None and prev_year_c12 != 0:
                c12_yoy = (c12 / prev_year_c12 - 1.0) * 100.0

        # ---------- 중량 및 단가 ----------
        kg_yoy = None
        prev_year_kg = kg_all.get(yyyymm_prev_year)
        if prev_year_kg is not None and prev_year_kg != 0 and kg is not None:
            kg_yoy = (kg / prev_year_kg - 1.0) * 100.0

        unit_price = None
        if usd is not None and kg is not None and kg != 0:
            unit_price = usd / kg

        unit_yoy = None
        if (
            unit_price is not None
            and prev_year_usd is not None
            and prev_year_kg is not None
            and prev_year_kg != 0
        ):
            prev_unit = prev_year_usd / prev_year_kg
            if prev_unit != 0:
                unit_yoy = (unit_price / prev_unit - 1.0) * 100.0

        row = [
            period_str,        # 기간
            round(usd) if usd is not None else None,          # 수출금액
            fmt_signed(usd_yoy),                              # YoY(%)
            fmt_signed(usd_mom),                              # MoM(%)
            round(c12) if c12 is not None else None,          # 누적12M(수출금액)
            fmt_signed(c12_yoy),                              # 누적12M YoY(%)
            fmt_signed(c12_mom),                              # 누적12M MoM(%)
            round(kg) if kg is not None else None,            # 수출중량
            fmt_signed(kg_yoy),                               # 수출중량 YoY(%)
            round(unit_price, 1) if unit_price is not None else None,  # 수출단가
            fmt_signed(unit_yoy),                             # 수출단가 YoY(%)
        ]
        rows.append(row)

    return rows


# =========================================================
# 엑셀 값만 갈아끼우기 (차트/서식 유지)
#  - A1:K37 : 메인 36개월 테이블
#  - C40:F42 : 스냅샷(10/20/30일 실적 & 환산치)
# =========================================================

HEADERS = [
    "기간",
    "수출금액",
    "YoY(%)",
    "MoM(%)",
    "누적12M(수출금액)",
    "누적12M YoY(%)",
    "누적12M MoM(%)",
    "수출중량",
    "수출중량 YoY(%)",
    "수출단가",
    "수출단가 YoY(%)",
]


def write_table_to_excel(
    src_path: Path,
    out_path: Path,
    rows: list[list],
    snapshot_rows: list[dict],
):
    """
    Excel COM(pywin32)을 사용해서:
    - SrcFile을 엑셀로 직접 열고
    - A1:K37 값만 덮어쓴 뒤
    - C40:F42 스냅샷 영역을 초기화 후 필요 스냅만 채운 뒤
    - OutCopy로 다른 이름 저장

    snapshot_rows 예:
      [
        {"row": 40, "usd_raw": ..., "kg_raw": ..., "usd_est": ..., "kg_est": ...},
        {"row": 41, ...},
        {"row": 42, ...},
      ]

    → 차트/도형/서식은 Excel이 직접 처리하므로 그대로 유지됨
    """
    if not src_path.exists():
        raise FileNotFoundError(f"SrcFile을 찾을 수 없습니다: {src_path}")

    if len(rows) != WINDOW_MONTHS:
        raise RuntimeError(f"36개월 테이블이어야 합니다. 현재 행 수: {len(rows)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 이전 결과 파일이 있으면 먼저 삭제 시도
    if out_path.exists():
        try:
            out_path.unlink()
        except PermissionError:
            raise RuntimeError(
                f"OutCopy 대상 파일이 이미 열려 있어 삭제할 수 없습니다.\n"
                f"엑셀에서 '{out_path.name}' 파일(또는 같은 이름의 통합문서)을 모두 닫고 다시 실행하세요."
            )

    excel = win32.Dispatch("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False

    try:
        wb = excel.Workbooks.Open(str(src_path))
        ws = wb.Worksheets(1)  # 필요하면 시트명으로 변경 가능

        # ---------- 메인 테이블: A1:K37 ----------
        # 헤더 A1:K1
        for col_idx, header in enumerate(HEADERS, start=1):
            ws.Cells(1, col_idx).Value = header

        # 데이터 2~37행
        for row_idx, row_values in enumerate(rows, start=2):
            for col_idx, val in enumerate(row_values, start=1):
                ws.Cells(row_idx, col_idx).Value = val

        # ---------- 스냅샷 영역: C40:F42 ----------
        # 1) 전체 Clear
        ws.Range("C40:F42").ClearContents()

        # 2) snapshot_rows 데이터 채우기
        for snap_row in snapshot_rows:
            r = snap_row["row"]  # 40 / 41 / 42
            ws.Cells(r, 3).Value = snap_row["usd_raw"]   # C: 수출금액(원자료)
            ws.Cells(r, 4).Value = snap_row["kg_raw"]    # D: 수출중량(원자료)
            ws.Cells(r, 5).Value = snap_row["usd_est"]   # E: 환산수출금액
            ws.Cells(r, 6).Value = snap_row["kg_est"]    # F: 환산중량

        try:
            # FileFormat=51 : xlsx
            wb.SaveAs(str(out_path), FileFormat=51)
        except Exception as e:
            raise RuntimeError(
                f"SaveAs 실패: {out_path}\n"
                f"같은 이름의 통합문서가 엑셀에서 이미 열려 있는지 확인하세요."
            ) from e
        finally:
            wb.Close(SaveChanges=False)
    finally:
        excel.Quit()


# =========================================================
# 개별 작업 처리
# =========================================================

def process_job(
    trass_conn: sqlite3.Connection,
    item_conn: sqlite3.Connection,
    ref_date: datetime,
    job_row: pd.Series,
):
    item_name = str(job_row["ItemName"]).strip()
    hs_raw = str(job_row["HS"]).strip()

    if not hs_raw:
        print(f"[SKIP] Item={item_name} : HS 코드 없음")
        return

    hs_list = [h.strip() for h in hs_raw.split(",") if h.strip()]

    # 파일 경로 처리
    src_file = Path(str(job_row["SrcFile"]).strip())
    out_file = Path(str(job_row["OutCopy"]).strip())

    if not src_file.is_absolute():
        src_file = BASE_DIR / src_file
    if not out_file.is_absolute():
        out_file = BASE_DIR / out_file

    # 기준월 및 snap
    yyyymm = ref_date.strftime("%Y%m")
    snap = infer_snap_from_day(ref_date.day)

    print(f"\n=== Item: {item_name} | 기준월={yyyymm}, snap={snap}, HS={hs_list} ===")
    print(f"  - SrcFile: {src_file}")
    print(f"  - OutCopy: {out_file}")

    # -----------------------------------------------------
    # 1) 10/20/30일 스냅샷용 + 기준월 추정치용 데이터 한 번에 가져오기
    # -----------------------------------------------------
    snap_order = ["10", "20", "30"]
    if snap == "10":
        snaps_to_use = ["10"]
    elif snap == "20":
        snaps_to_use = ["10", "20"]
    else:
        snaps_to_use = ["10", "20", "30"]

    # snap별 원자료 + 환산치
    snap_info: dict[str, dict] = {}

    for s in snaps_to_use:
        usd_raw, kg_raw, totaldays, workdays = get_provisional_agg(
            trass_conn, yyyymm, s, hs_list
        )
        factor = float(totaldays) / float(workdays)
        usd_est = usd_raw * factor
        kg_est = kg_raw * factor

        snap_info[s] = {
            "usd_raw": usd_raw,
            "kg_raw": kg_raw,
            "totaldays": totaldays,
            "workdays": workdays,
            "factor": factor,
            "usd_est": usd_est,
            "kg_est": kg_est,
        }

    # 기준월 추정치는 "현재 snap" 기준
    cur = snap_info[snap]
    usd_est_final = cur["usd_est"]
    kg_est_final = cur["kg_est"]

    print(
        f"  - [{snap}일 기준] 잠정치 합계: usd_raw={cur['usd_raw']:,.0f}, "
        f"kg_raw={cur['kg_raw']:,.0f}"
    )
    print(
        f"  - totaldays={cur['totaldays']}, workdays={cur['workdays']}, "
        f"factor={cur['factor']:.4f}"
    )
    print(
        f"  - 월간 추정치: usd_est={usd_est_final:,.0f}, "
        f"kg_est={kg_est_final:,.0f}"
    )

    # -----------------------------------------------------
    # 2) item_exports 전체 시계열 + 35개월 윈도우
    # -----------------------------------------------------
    full_df = get_item_monthly_series(item_conn, hs_list)
    df_35 = build_36m_window(full_df, yyyymm)

    # 3) 기준월(추정치) 행 추가 → 36개월 DataFrame
    df_36 = df_35.copy()
    df_36 = pd.concat(
        [
            df_36,
            pd.DataFrame(
                [{"yyyymm": yyyymm, "usd": usd_est_final, "kg": kg_est_final}]
            ),
        ],
        ignore_index=True,
    )
    df_36 = df_36.sort_values("yyyymm").reset_index(drop=True)

    if len(df_36) != WINDOW_MONTHS:
        raise RuntimeError(
            f"[{item_name}] 36개월 윈도우가 아닙니다. 현재 행 수: {len(df_36)}"
        )

    # 4) 36개월 테이블 계산 (YoY/MoM/12M/단가)
    rows = build_metrics_table(full_df, df_36)

    # -----------------------------------------------------
    # 3) 엑셀 스냅샷용 C40:F42 데이터 구성
    # -----------------------------------------------------
    snap_to_row = {"10": 40, "20": 41, "30": 42}
    snapshot_rows: list[dict] = []

    for s in snaps_to_use:
        info = snap_info[s]
        snapshot_rows.append(
            {
                "row": snap_to_row[s],
                "usd_raw": info["usd_raw"],
                "kg_raw": info["kg_raw"],
                "usd_est": info["usd_est"],
                "kg_est": info["kg_est"],
            }
        )

    # -----------------------------------------------------
    # 4) 엑셀 파일에 A1:K37 + C40:F42 값만 덮어쓰기 후 OutCopy로 저장
    # -----------------------------------------------------
    write_table_to_excel(src_file, out_file, rows, snapshot_rows)

    print(f"  - 엑셀 저장 완료 (그래프/서식 유지): {out_file}")


# =========================================================
# 메인 실행
# =========================================================

def main():
    # 코드 상단의 REF_DATE_STR 사용
    ref_date = parse_ref_date(REF_DATE_STR)
    print(f"기준일: {ref_date.date()} (코드 상단 REF_DATE_STR에서 설정)")

    # 작업리스트 읽기
    if not WORKLIST_PATH.exists():
        raise FileNotFoundError(f"작업리스트 파일을 찾을 수 없습니다: {WORKLIST_PATH}")

    df_jobs = pd.read_excel(WORKLIST_PATH)

    # Enabled == 1 필터
    if "Enabled" not in df_jobs.columns:
        raise RuntimeError("작업리스트에 'Enabled' 헤더가 없습니다.")

    df_jobs = df_jobs[df_jobs["Enabled"] == 1].copy()
    if df_jobs.empty:
        print("Enabled=1 인 작업이 없습니다. 종료합니다.")
        return

    # ✅ 이제 Totaldays, Workdays는 DB에서 읽으므로 작업리스트에서 요구하지 않음
    required_cols = ["ItemName", "HS", "SrcFile", "OutCopy"]
    for col in required_cols:
        if col not in df_jobs.columns:
            raise RuntimeError(f"작업리스트에 '{col}' 헤더가 없습니다.")

    # DB 연결
    trass_conn = get_connection(TRASS_DB_PATH)
    item_conn = get_connection(ITEM_DB_PATH)

    try:
        total_jobs = len(df_jobs)
        print(f"\n총 {total_jobs}개 작업 실행 (기준일자: {ref_date.date()})")

        for idx, (_, row) in enumerate(df_jobs.iterrows(), start=1):
            print(f"\n[{idx}/{total_jobs}] 작업 시작")
            process_job(trass_conn, item_conn, ref_date, row)

        print("\n모든 작업 완료.")

    finally:
        trass_conn.close()
        item_conn.close()


if __name__ == "__main__":
    main()

