"""
KML Tools Suite  —  โปรแกรมจัดการไฟล์ KML สำหรับองค์กร
Stop button: kills worker thread immediately via ctypes
"""

import os, re, copy, tempfile, ctypes
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

# ─────────────────────────────────────────────────────────────
# Thread killer — raises exception inside a running thread
# ─────────────────────────────────────────────────────────────
def _kill_thread(thread):
    """Raise SystemExit inside the target thread to stop it immediately."""
    if thread is None or not thread.is_alive():
        return
    tid = thread.ident
    if tid is None:
        return
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(tid),
        ctypes.py_object(SystemExit)
    )
    if res == 0:
        pass  # thread already finished
    elif res > 1:
        # Undo if too many threads were affected
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(tid), None)


# ─────────────────────────────────────────────────────────────
# BACKEND — MODULE 1 : Clip & Combine KML
# ─────────────────────────────────────────────────────────────
from lxml import etree
from shapely.geometry import (Point, Polygon, LineString,
                               MultiLineString, MultiPolygon,
                               GeometryCollection)


NS = "http://www.opengis.net/kml/2.2"

def _parse_kml(path):
    """Parse KML robustly with recover=True."""
    parser = etree.XMLParser(recover=True, remove_comments=True)
    return etree.parse(path, parser)


def _tag_matches(el, local_name):
    tag = el.tag
    if isinstance(tag, str):
        local = tag.split("}")[-1] if "}" in tag else tag
        return local == local_name
    return False


def _find_all(root, tag):
    """Find ALL descendants — any namespace, any folder depth."""
    return [el for el in root.iter() if _tag_matches(el, tag)]


def _find(el, tag):
    """Find first descendant — any namespace."""
    for child in el.iter():
        if child is el: continue
        if _tag_matches(child, tag): return child
    return None


def _find_direct(el, tag):
    """Find first direct child — any namespace."""
    for child in el:
        if _tag_matches(child, tag): return child
    return None


def clip_geometry(input_kml, boundary_polygon, output_kml):
    try:
        tree = _parse_kml(input_kml)
        root = tree.getroot()
        remove = []
        for pm in _find_all(root, "Placemark"):
            pt = _find(pm, "Point")
            if pt is not None:
                ce = _find(pt, "coordinates")
                if ce is not None:
                    lon, lat = map(float, ce.text.strip().split(',')[:2])
                    if not Point(lon, lat).within(boundary_polygon):
                        remove.append(pm)
                continue
            le = _find(pm, "LineString")
            pe = _find(pm, "Polygon")
            ce = None; geom = None
            if le is not None:
                ce = _find(le, "coordinates")
                if ce is not None:
                    pts = [tuple(map(float, c.split(',')[:2])) for c in ce.text.strip().split()]
                    if len(pts) > 1: geom = LineString(pts)
            elif pe is not None:
                ob = _find(pe, "outerBoundaryIs")
                if ob is not None:
                    ce = _find(ob, "coordinates")
                if ce is not None:
                    pts = [tuple(map(float, c.split(',')[:2])) for c in ce.text.strip().split()]
                    if len(pts) > 2: geom = Polygon(pts)
            if geom:
                clipped = geom.intersection(boundary_polygon)
                if clipped.is_empty:
                    remove.append(pm)
                else:
                    def _col(g):
                        if isinstance(g, Polygon): return list(g.exterior.coords)
                        if isinstance(g, LineString): return list(g.coords)
                        return []
                    pts2 = []
                    if isinstance(clipped, (Polygon, LineString)): pts2 = _col(clipped)
                    elif isinstance(clipped, (MultiPolygon, MultiLineString, GeometryCollection)):
                        for p in clipped.geoms: pts2 += _col(p)
                    if pts2 and ce is not None:
                        ce.text = " ".join(f"{x},{y}" for x, y in pts2)
        for pm in remove: pm.getparent().remove(pm)
        doc = _find(root, "Document")
        if doc is not None:
            nm = _find_direct(doc, "name")
            if nm is None: nm = etree.SubElement(doc, f"{{{NS}}}name")
            nm.text = os.path.basename(output_kml)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.kml') as t:
            tree.write(t.name, encoding="utf-8", xml_declaration=True)
            return t.name
    except Exception as e:
        print(f"clip error: {e}"); return None


def combine_kml_files(output_files, input_file_name, boundary_kml):
    out = tempfile.NamedTemporaryFile(delete=False, suffix='.kml')
    kml = etree.Element(f"{{{NS}}}kml")
    doc = etree.SubElement(kml, f"{{{NS}}}Document")
    etree.SubElement(doc, f"{{{NS}}}name").text = os.path.splitext(input_file_name)[0]
    s1 = etree.SubElement(doc, f"{{{NS}}}Style", id="normalPlacemark")
    ls = etree.SubElement(s1, f"{{{NS}}}LineStyle")
    etree.SubElement(ls, f"{{{NS}}}color").text = "ff0000ff"
    etree.SubElement(ls, f"{{{NS}}}width").text = "2"
    ps = etree.SubElement(s1, f"{{{NS}}}PolyStyle")
    etree.SubElement(ps, f"{{{NS}}}color").text = "7f0000ff"
    etree.SubElement(ps, f"{{{NS}}}outline").text = "1"
    s2 = etree.SubElement(doc, f"{{{NS}}}Style", id="boundaryPlacemark")
    lb = etree.SubElement(s2, f"{{{NS}}}LineStyle")
    etree.SubElement(lb, f"{{{NS}}}color").text = "ff00ff00"
    etree.SubElement(lb, f"{{{NS}}}width").text = "3"
    pb2 = etree.SubElement(s2, f"{{{NS}}}PolyStyle")
    etree.SubElement(pb2, f"{{{NS}}}color").text = "0000ff00"
    etree.SubElement(pb2, f"{{{NS}}}fill").text = "1"
    etree.SubElement(pb2, f"{{{NS}}}outline").text = "1"

    # boundary dict — use recover parser here too
    bt = _parse_kml(boundary_kml).getroot()
    bdict = {}
    for b in _find_all(bt, "Placemark"):
        nm = _find(b, "name")
        if nm is not None and nm.text: bdict[nm.text.strip()] = b

    for file, area in output_files:
        try:
            root = _parse_kml(file).getroot()
            folder = etree.SubElement(doc, f"{{{NS}}}Folder")
            etree.SubElement(folder, f"{{{NS}}}name").text = area
            if area in bdict:
                bp = copy.deepcopy(bdict[area])
                etree.SubElement(bp, f"{{{NS}}}styleUrl").text = "#boundaryPlacemark"
                folder.append(bp)
            for pm in _find_all(root, "Placemark"):
                etree.SubElement(pm, f"{{{NS}}}styleUrl").text = "#normalPlacemark"
                folder.append(pm)
            os.unlink(file)
        except Exception as e: print(f"combine error {area}: {e}")
    etree.ElementTree(kml).write(out.name, encoding="utf-8", xml_declaration=True,
                                  pretty_print=True)
    return out.name


def process_clip_areas(input_kml, boundary_kml, output_kml, cb):
    # Use recover parser for boundary file too
    bt = _parse_kml(boundary_kml).getroot()
    boundaries = _find_all(bt, "Placemark")
    out_files = []; fname = os.path.basename(input_kml)
    for i, bp in enumerate(boundaries):
        bc = _find(bp, "coordinates")
        if bc is not None:
            try:
                pts = [(float(c.split(',')[0]), float(c.split(',')[1]))
                       for c in bc.text.strip().split() if c.strip()]
            except Exception:
                continue
            if len(pts) < 3:
                continue
            poly = Polygon(pts)
            nm = _find(bp, "name")
            area = (nm.text.strip() if nm is not None and nm.text else f"area_{i+1}")
            cb(f"กำลังประมวลผล: {area} ({i+1}/{len(boundaries)})")
            clipped = clip_geometry(input_kml, poly, f"{area}.kml")
            if clipped: out_files.append((clipped, area))
    if out_files:
        cb("กำลังรวมไฟล์ ...")
        combined = combine_kml_files(out_files, fname, boundary_kml)
        with open(combined, 'rb') as s, open(output_kml, 'wb') as d: d.write(s.read())
        os.unlink(combined)
        return len(out_files)
    return 0


# ─────────────────────────────────────────────────────────────
# BACKEND — MODULE 2 : Separate Duplicate Lines
# ─────────────────────────────────────────────────────────────
def meters_to_deg(m): return m / 111320.0

def offset_coords(coords, idx, step):
    res = []
    for c in coords:
        lon, lat, *alt = map(float, c.split(','))
        res.append(f"{lon+step*idx},{lat+step*idx},{alt[0] if alt else 0}")
    return res

def copy_pm(pm):
    new = etree.Element(f"{{{NS}}}Placemark")
    for k, v in pm.attrib.items(): new.set(k, v)
    for ch in pm: new.append(ch)
    return new

def add_ext_desc(pm):
    if _find(pm, "description") is not None: return
    ed = _find(pm, "ExtendedData")
    if ed is None: return
    rows = []
    for d in ed.findall(f"{{{NS}}}Data"):
        n = d.get("name"); v = d.find(f"{{{NS}}}value")
        if n and v is not None: rows.append(f"<tr><td>{n}</td><td>{v.text}</td></tr>")
    if rows:
        etree.SubElement(pm, f"{{{NS}}}description").text = \
            f"<table border='1' style='border-collapse:collapse;'>{''.join(rows)}</table>"

def proc_single(data):
    pm, coords, idx, step = data
    new = copy_pm(pm)
    ls = _find(new, "LineString")
    if ls is not None:
        ce = _find(ls, "coordinates")
        if ce is not None: ce.text = "\n".join(offset_coords(coords, idx, step))
    add_ext_desc(new); return new

def separate_duplicate_lines(input_kml, output_folder, dist_m=2, cb=None):
    step = meters_to_deg(dist_m)
    fname = os.path.splitext(os.path.basename(input_kml))[0]
    ctx = etree.iterparse(input_kml, events=("end",),
                           tag=f"{{{NS}}}Placemark", recover=True)
    pms = []
    for _, el in ctx:
        ls = _find(el, "LineString")
        if ls is not None:
            ce = _find(ls, "coordinates")
            if ce is not None: pms.append((el, ce.text.strip().split()))
    cmap = {}
    for pm, coords in pms: cmap.setdefault(tuple(coords), []).append(pm)
    overlapping = [g for g in cmap.values() if len(g) > 1]
    unique      = [g for g in cmap.values() if len(g) == 1]
    if cb: cb(f"พบเส้นซ้ำ {len(overlapping)} กลุ่ม / เส้นไม่ซ้ำ {len(unique)} เส้น")
    new_pms = []
    for grp in overlapping:
        first = copy_pm(grp[0]); add_ext_desc(first); new_pms.append(first)
        dlist = []
        for i in range(1, len(grp)):
            pm_i = grp[i]
            ls_i = _find(pm_i, "LineString")
            if ls_i is None: continue
            ce_i = _find(ls_i, "coordinates")
            if ce_i is None: continue
            dlist.append((pm_i, ce_i.text.strip().split(), i, step))
        if dlist:
            with ThreadPoolExecutor(max_workers=4) as ex:
                new_pms.extend(list(ex.map(proc_single, dlist)))
    for grp in unique:
        p = copy_pm(grp[0]); add_ext_desc(p); new_pms.append(p)
    new_doc = etree.Element(f"{{{NS}}}kml")
    folder  = etree.SubElement(new_doc, f"{{{NS}}}Folder")
    for p in new_pms: folder.append(p)
    out = os.path.join(output_folder, f"{fname}_processed.kml")
    with open(out, 'wb') as f:
        f.write(etree.tostring(new_doc, pretty_print=True, xml_declaration=True, encoding='UTF-8'))
    return out


# ─────────────────────────────────────────────────────────────
# BACKEND — MODULE 3 : Excel → KML
# ─────────────────────────────────────────────────────────────
import pandas as pd
import simplekml

def parse_coord(s):
    try:
        m = re.findall(r"[-+]?\d*\.\d+|[-+]?\d+", s)
        if len(m) != 2: return None
        lat, lon = map(float, m); return lon, lat
    except: return None

def excel_to_kml(excel_file, sheet, output_folder, cb=None):
    data = pd.read_excel(excel_file, sheet_name=sheet)
    data.columns = data.columns.str.strip()
    if cb: cb("กำลังแปลงพิกัด ...")
    data['พิกัด'] = data['พิกัด'].apply(lambda x: parse_coord(str(x)) if pd.notna(x) else None)
    data = data.dropna(subset=['พิกัด'])
    if cb: cb("กำลังสร้างไฟล์ KML ...")
    kml = simplekml.Kml()
    for did, grp in data.groupby('id'):
        coords = grp.sort_values('ลำดับพิกัด')['พิกัด'].tolist()
        folder = kml.newfolder(name=f"{grp['ชื่อชุมสาย'].iloc[0]} {did}")
        ls = folder.newlinestring(name=f"{grp['ชื่อชุมสาย'].iloc[0]} {did}")
        ls.coords = coords
        ls.style.linestyle.color = simplekml.Color.blue
        ls.style.linestyle.width = 3
        ls.description = "\n".join([f"{c}: {grp[c].iloc[0]}" for c in data.columns if c != 'พิกัด'])
    out = os.path.join(output_folder, os.path.splitext(os.path.basename(excel_file))[0] + ".kml")
    kml.save(out); return out


# ─────────────────────────────────────────────────────────────
# BACKEND — MODULE 4 : Missing Coords from Excel → KML
# ─────────────────────────────────────────────────────────────
def _read_excel_coords(excel_file, sheet):
    """อ่าน DataFrame จากไฟล์ Excel พร้อมหาคอลัมน์พิกัดอัตโนมัติ"""
    def _find_coord_col(df):
        for col in df.columns:
            sample = str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else ""
            if re.search(r"\d+\.\d+.*,.*\d+\.\d+", sample):
                return col
        return None

    def _parse(s):
        try:
            m = re.findall(r"[-+]?\d*\.\d+|[-+]?\d+", str(s))
            if len(m) >= 2:
                return float(m[1]), float(m[0])   # (lon, lat)
        except Exception:
            pass
        return None

    df = pd.read_excel(excel_file, sheet_name=sheet, header=0)
    df.columns = df.columns.str.strip()
    coord_col = _find_coord_col(df)
    if coord_col is None:
        df = pd.read_excel(excel_file, sheet_name=sheet, header=1)
        df.columns = df.columns.str.strip()
        coord_col = _find_coord_col(df)
    if coord_col is None:
        raise ValueError(f"ไม่พบคอลัมน์พิกัด (lat,lon) ในไฟล์ {os.path.basename(excel_file)}")

    df["_coord"] = df[coord_col].apply(lambda x: _parse(x) if pd.notna(x) else None)
    df = df.dropna(subset=["_coord"]).reset_index(drop=True)
    if df.empty:
        raise ValueError(f"ไม่พบพิกัดที่ถูกต้องในไฟล์ {os.path.basename(excel_file)}")

    tag_col = next((c for c in df.columns
                    if "tag" in c.lower() or "เสา" in c.lower()), None)
    no_col  = next((c for c in df.columns
                    if c.lower() in ("no.", "no", "ลำดับ", "no_")), None)
    return df, tag_col, no_col


def _add_excel_to_folder(lines_folder, points_folder,
                         df, tag_col, no_col, ls_name, show_points):
    """เพิ่ม LineString ลง lines_folder และ Points ลง points_folder"""
    coords_list = df["_coord"].tolist()
    if not coords_list:
        return

    # ── เส้น (ชื่อเส้น = ls_name เดียวกับโฟลเดอร์หลัก) ────
    ls = lines_folder.newlinestring(name=ls_name)
    ls.coords = coords_list
    ls.style.linestyle.color = simplekml.Color.blue
    ls.style.linestyle.width = 3
    ls.description = ls_name

    # ── หมุด ────────────────────────────────────────────────
    if show_points and points_folder is not None:
        for i, row in df.iterrows():
            if tag_col and pd.notna(row.get(tag_col)):
                pt_name = str(row[tag_col])
            elif no_col and pd.notna(row.get(no_col)):
                pt_name = f"จุดที่ {row[no_col]}"
            else:
                pt_name = f"จุดที่ {i + 1}"
            pt = points_folder.newpoint(name=pt_name, coords=[row["_coord"]])
            pt.style.iconstyle.color = simplekml.Color.red
            pt.description = pt_name


def missing_coords_excel_to_kml(excel_files, sheets, line_name,
                                output_path, show_points=True, cb=None):
    """
    รับ list ของ excel_file + sheet แล้วสร้าง KML ไฟล์เดียว:
      - โฟลเดอร์หลัก = line_name ที่ผู้ใช้ระบุ
      - sub-folder "เส้นทาง"  — LineString ทุกไฟล์
      - sub-folder "หมุด"     — Points ทุกไฟล์ (ถ้าเลือก)
    """
    kml          = simplekml.Kml()
    root_name    = line_name.strip() if line_name.strip() else "พิกัดที่ตกหล่น"
    root_folder  = kml.newfolder(name=root_name)
    lines_folder = root_folder.newfolder(name="สายไฟ")
    pts_folder   = root_folder.newfolder(name="เสาไฟ") if show_points else None
    errors       = []

    for i, (excel_file, sheet) in enumerate(zip(excel_files, sheets)):
        fname = os.path.splitext(os.path.basename(excel_file))[0]
        if cb: cb(f"[{i+1}/{len(excel_files)}] กำลังอ่าน: {fname} ...")
        try:
            df, tag_col, no_col = _read_excel_coords(excel_file, sheet)
            _add_excel_to_folder(lines_folder, pts_folder,
                                 df, tag_col, no_col, root_name, show_points)
        except Exception as e:
            errors.append(f"• {fname}: {e}")

    if errors and len(errors) == len(excel_files):
        raise ValueError("ไม่สามารถอ่านไฟล์ได้เลย:\n" + "\n".join(errors))

    if cb: cb("กำลังบันทึกไฟล์ KML ...")
    kml.save(output_path)
    return output_path, errors


# ─────────────────────────────────────────────────────────────
# BACKEND — MODULE 5 : Manual Bracket Coords → KML
# ─────────────────────────────────────────────────────────────
_DESC_HEADERS = [
    "id", "วันที่สร้าง", "รหัสอุปกรณ์", "ชื่ออุปกรณ์", "รายละเอียด",
    "unit_in", "รุ่น", "ยี่ห้อ", "ชื่อโปรเจค", "ชื่อศูนย์บริการ",
    "ชื่อชุมสาย", "type", "ระยะทาง", "ลำดับพิกัด",
]

def manual_coords_to_kml(coord_str, output_path, desc_dict=None,
                         show_points=False, cb=None):
    """
    รับพิกัดรูปแบบวงเล็บ  (lat,lon)(lat,lon)...
    - โฟลเดอร์หลัก = ชื่อไฟล์
    - sub-folder "เส้นทาง"
    - sub-folder "หมุด" (ถ้าเลือก)
    """
    if cb: cb("กำลังแปลงพิกัด ...")
    pairs = re.findall(r"\(([^)]+)\)", coord_str)
    coords = []
    for p in pairs:
        m = re.findall(r"[-+]?\d*\.\d+|[-+]?\d+", p)
        if len(m) >= 2:
            lat, lon = float(m[0]), float(m[1])
            coords.append((lon, lat))
    if not coords:
        raise ValueError("ไม่พบพิกัดที่ถูกต้อง — ตรวจสอบรูปแบบ (lat,lon)(lat,lon)")

    if cb: cb(f"พบ {len(coords)} จุด — กำลังสร้าง KML ...")
    fname        = os.path.splitext(os.path.basename(output_path))[0]
    kml          = simplekml.Kml()
    root_folder  = kml.newfolder(name=fname)
    lines_folder = root_folder.newfolder(name="เส้นทาง")

    # ── เส้น ────────────────────────────────────────────────
    ls = lines_folder.newlinestring(name=fname)
    ls.coords = coords
    ls.style.linestyle.color = simplekml.Color.blue
    ls.style.linestyle.width = 3
    if desc_dict:
        desc_text = "\n".join(f"{k}: {v}" for k, v in desc_dict.items() if v.strip())
        if desc_text:
            ls.description = desc_text

    # ── หมุด ────────────────────────────────────────────────
    if show_points:
        pts_folder = root_folder.newfolder(name="หมุด")
        for i, coord in enumerate(coords):
            pt = pts_folder.newpoint(name=f"จุดที่ {i + 1}", coords=[coord])
            pt.style.iconstyle.color = simplekml.Color.red

    if cb: cb("กำลังบันทึกไฟล์ ...")
    kml.save(output_path)
    return output_path
# ═════════════════════════════════════════════════════════════
P = {
    "white":     "#FFFFFF",
    "bg":        "#F3F4F8",
    "bg2":       "#ECEEF4",
    "border":    "#D6D9E4",
    "text":      "#1C1E2A",
    "text_mid":  "#52566A",
    "text_dim":  "#9197B3",
    "blue":      "#1A56DB",
    "blue_bg":   "#EEF3FD",
    "indigo":    "#5145CD",
    "indigo_bg": "#F0EEFB",
    "teal":      "#0E7E6E",
    "teal_bg":   "#E6F4F2",
    "warn_fg":   "#8A4B00",
    "warn_bg":   "#FEF6E7",
    "ok":        "#0E7E6E",
    "err":       "#C0282A",
    "stop":      "#B91C1C",
    "stop_bg":   "#FEF2F2",
}

F = {
    "title":   ("Segoe UI", 11, "bold"),
    "body":    ("Segoe UI", 9),
    "bold":    ("Segoe UI", 9, "bold"),
    "nav":     ("Segoe UI", 9),
    "nav_act": ("Segoe UI", 9, "bold"),
    "btn":     ("Segoe UI", 9, "bold"),
    "app":     ("Segoe UI", 10, "bold"),
    "small":   ("Segoe UI", 8),
}

def lbl(p, text, f=None, fg=None, bg=None, **kw):
    return tk.Label(p, text=text, font=f or F["body"],
                    fg=fg or P["text"], bg=bg or P["white"], **kw)

def rule(p, bg=None, py=6):
    tk.Frame(p, bg=bg or P["border"], height=1).pack(fill="x", pady=py)


# ─────────────────────────────────────────────────────────────
# Widgets
# ─────────────────────────────────────────────────────────────
class FilePick(tk.Frame):
    def __init__(self, parent, label, btn_text, cmd, accent=None, bg=P["bg"]):
        super().__init__(parent, bg=bg)
        self.pack(fill="x", pady=3)
        self._path = ""
        self.var = tk.StringVar(value="ยังไม่ได้เลือก")
        accent = accent or P["blue"]
        lbl(self, label, f=F["bold"], bg=bg, width=20, anchor="w").pack(side="left")
        self._pill = tk.Label(self, textvariable=self.var, font=F["body"],
                              bg=P["white"], fg=P["text_dim"], anchor="w",
                              width=28, padx=8, pady=4,
                              highlightbackground=P["border"], highlightthickness=1)
        self._pill.pack(side="left", padx=(0, 6))
        tk.Button(self, text=btn_text, command=cmd, font=F["btn"],
                  bg=P["white"], fg=accent, relief="flat", bd=0,
                  padx=10, pady=4, cursor="hand2",
                  highlightbackground=accent, highlightthickness=1,
                  activebackground=P["bg2"], activeforeground=accent).pack(side="left")

    def set(self, full, display=None):
        self._path = full
        self.var.set(display or os.path.basename(full) or full)
        self._pill.config(fg=P["text"])

    def get(self): return self._path

    def reset(self):
        self._path = ""
        self.var.set("ยังไม่ได้เลือก")
        self._pill.config(fg=P["text_dim"])


class NoteStrip(tk.Frame):
    def __init__(self, parent, lines, accent=None, bg=None):
        accent = accent or P["blue"]
        bg     = bg     or P["blue_bg"]
        super().__init__(parent, bg=bg,
                         highlightbackground=P["border"], highlightthickness=1)
        self.pack(fill="x", pady=(0, 8))
        tk.Frame(self, bg=accent, width=3).pack(side="left", fill="y")
        inner = tk.Frame(self, bg=bg, padx=10, pady=7)
        inner.pack(side="left", fill="both", expand=True)
        for line in lines:
            lbl(inner, line, f=F["body"], fg=P["text_mid"], bg=bg).pack(anchor="w")


class StatusBar(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg=P["bg"],
                         highlightbackground=P["border"], highlightthickness=1)
        self.pack(fill="x", side="bottom")
        self._dot = lbl(self, " o ", f=F["small"], fg=P["text_dim"], bg=P["bg"])
        self._dot.pack(side="left", padx=(8, 0))
        self._msg = lbl(self, "พร้อมใช้งาน", f=F["small"], fg=P["text_dim"], bg=P["bg"])
        self._msg.pack(side="left", pady=5)

    def set(self, text, kind="idle"):
        c = {"idle": P["text_dim"], "run": P["blue"], "ok": P["ok"],
             "err":  P["err"],      "warn": P["warn_fg"], "stop": P["stop"]
             }.get(kind, P["text_dim"])
        dot = {"run": "●", "ok": "●", "err": "●", "warn": "●", "stop": "■"}.get(kind, "○")
        self._msg.config(text=text, fg=c)
        self._dot.config(text=f" {dot} ", fg=c)


# ─────────────────────────────────────────────────────────────
# Button factories
# ─────────────────────────────────────────────────────────────
def primary_btn(parent, text, cmd, color=None, **kw):
    color = color or P["blue"]
    return tk.Button(parent, text=text, command=cmd, font=F["btn"],
                     bg=color, fg=P["white"], relief="flat", bd=0,
                     padx=18, pady=7, cursor="hand2",
                     activebackground=P["bg2"], activeforeground=color, **kw)

def danger_btn(parent, text, cmd, **kw):
    """Red-tinted stop button — always visible, toggles disabled state."""
    return tk.Button(parent, text=text, command=cmd, font=F["btn"],
                     bg=P["stop_bg"], fg=P["stop"], relief="flat", bd=0,
                     padx=14, pady=7, cursor="hand2",
                     highlightbackground=P["stop"], highlightthickness=1,
                     activebackground="#fee2e2", activeforeground=P["stop"],
                     disabledforeground="#e5a3a3", **kw)

def ghost_btn(parent, text, cmd, **kw):
    return tk.Button(parent, text=text, command=cmd, font=F["btn"],
                     bg=P["white"], fg=P["text_mid"], relief="flat", bd=0,
                     padx=14, pady=7, cursor="hand2",
                     highlightbackground=P["border"], highlightthickness=1,
                     activebackground=P["bg2"], activeforeground=P["text"], **kw)


# ═════════════════════════════════════════════════════════════
#  BASE TAB — hard-kill stop via ctypes
# ═════════════════════════════════════════════════════════════
class BaseTab(tk.Frame):
    ACCENT = P["blue"]

    def __init__(self, parent):
        super().__init__(parent, bg=P["white"])
        self._worker: Thread | None = None
        self._st = StatusBar(self)   # bottom-first
        self._build()

    def _build(self): pass

    # ── state helpers ────────────────────────────────────────
    def _set_running(self):
        self._btn_start.config(state="disabled")
        self._btn_stop.config(state="normal")
        self._btn_reset.config(state="disabled")
        self._pb.start(12)

    def _set_idle(self):
        self._pb.stop()
        self._btn_start.config(state="normal")
        self._btn_stop.config(state="disabled")
        self._btn_reset.config(state="normal")
        self._worker = None

    # ── HARD STOP ────────────────────────────────────────────
    def _stop(self):
        """Kill worker thread immediately, reset UI."""
        if self._worker and self._worker.is_alive():
            _kill_thread(self._worker)
        self._worker = None
        self._set_idle()
        self._st.set("ยกเลิกการประมวลผลแล้ว  —  กดปุ่มประมวลผลเพื่อเริ่มใหม่", "stop")

    # ── runner ───────────────────────────────────────────────
    def _run(self, work_fn, done_fn, err_fn):
        self._set_running()

        def task():
            try:
                result = work_fn()
                self.after(0, lambda: done_fn(result))
            except SystemExit:
                # raised by _kill_thread — do nothing, UI already reset by _stop()
                pass
            except Exception as e:
                _e = e
                self.after(0, lambda err=_e: err_fn(err))

        self._worker = Thread(target=task, daemon=True)
        self._worker.start()

    # ── action row (start | stop | reset) ────────────────────
    def _build_action_row(self, parent, start_label, start_cmd, reset_cmd):
        row = tk.Frame(parent, bg=P["white"])
        row.pack(fill="x", pady=10)

        self._btn_start = primary_btn(row, start_label, start_cmd, color=self.ACCENT)
        self._btn_start.pack(side="left")

        self._btn_stop = danger_btn(row, "หยุดการประมวลผล", self._stop)
        self._btn_stop.config(state="disabled")
        self._btn_stop.pack(side="left", padx=8)

        self._btn_reset = ghost_btn(row, "รีเซ็ต", reset_cmd)
        self._btn_reset.pack(side="left")


# ═════════════════════════════════════════════════════════════
#  TAB 1 — Clip & Combine KML
# ═════════════════════════════════════════════════════════════
class ClipTab(BaseTab):
    ACCENT = P["blue"]

    def _build(self):
        body = tk.Frame(self, bg=P["white"], padx=24, pady=18)
        body.pack(fill="both", expand=True)

        lbl(body, "ตัดและรวม KML ตามขอบเขต",
            f=F["title"], bg=P["white"]).pack(anchor="w")
        lbl(body, "ตัดพิกัดจากไฟล์ KML ให้อยู่ใน Polygon แล้วรวมทุกพื้นที่เป็นไฟล์เดียว",
            f=F["body"], fg=P["text_dim"], bg=P["white"]).pack(anchor="w", pady=(2, 12))

        NoteStrip(body, [
            "1.  เลือกไฟล์ KML ต้นฉบับที่ต้องการตัดพื้นที่",
            "2.  เลือกไฟล์ขอบเขต (Boundary KML) ที่มี Polygon กำหนดแต่ละโซน",
            "3.  กำหนดตำแหน่งบันทึกไฟล์ผลลัพธ์",
            "4.  กด 'เริ่มประมวลผล' — โปรแกรมจะตัดและรวมไฟล์ให้อัตโนมัติ",
        ], accent=P["blue"], bg=P["blue_bg"])

        rule(body, py=4)

        card = tk.Frame(body, bg=P["bg"], highlightbackground=P["border"],
                        highlightthickness=1, padx=14, pady=12)
        card.pack(fill="x", pady=(0, 10))
        lbl(card, "เลือกไฟล์", f=F["bold"], fg=P["text_mid"],
            bg=P["bg"]).pack(anchor="w", pady=(0, 6))

        self._ip = FilePick(card, "ไฟล์ KML ต้นฉบับ",     "เลือกไฟล์",
                             self._si, accent=self.ACCENT, bg=P["bg"])
        self._bp = FilePick(card, "ไฟล์ขอบเขต (Boundary)", "เลือกไฟล์",
                             self._sb, accent=self.ACCENT, bg=P["bg"])
        self._op = FilePick(card, "บันทึกผลลัพธ์เป็น",     "กำหนดตำแหน่ง",
                             self._so, accent=self.ACCENT, bg=P["bg"])

        self._pb = ttk.Progressbar(body, mode="indeterminate", length=520)
        self._pb.pack(pady=(2, 0))

        self._build_action_row(body, "เริ่มประมวลผล", self._start, self._reset)

    def _si(self):
        p = filedialog.askopenfilename(filetypes=[("KML", "*.kml")])
        if p: self._ip.set(p)
    def _sb(self):
        p = filedialog.askopenfilename(filetypes=[("KML", "*.kml")])
        if p: self._bp.set(p)
    def _so(self):
        p = filedialog.asksaveasfilename(defaultextension=".kml",
                                          filetypes=[("KML", "*.kml")])
        if p: self._op.set(p)
    def _reset(self):
        for f in [self._ip, self._bp, self._op]: f.reset()
        self._st.set("พร้อมใช้งาน", "idle")

    def _start(self):
        if not all([self._ip.get(), self._bp.get(), self._op.get()]):
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณาเลือกไฟล์ให้ครบทั้ง 3 รายการ")
            return
        self._st.set("กำลังประมวลผล ...", "run")
        ip, bp, op = self._ip.get(), self._bp.get(), self._op.get()

        def work():
            return process_clip_areas(ip, bp, op,
                       lambda m: self._st.set(m, "run"))
        def done(n):
            self._set_idle()
            if n:
                self._st.set(f"สำเร็จ — {n} พื้นที่", "ok")
                messagebox.showinfo("สำเร็จ",
                    f"ประมวลผลเสร็จสิ้น  ({n} พื้นที่)\n\nบันทึกที่:\n{op}")
            else:
                self._st.set("ไม่พบข้อมูลที่ตรงเงื่อนไข", "warn")
                messagebox.showwarning("คำเตือน", "ไม่พบข้อมูลภายในขอบเขตที่กำหนด")
        def err(e):
            self._set_idle()
            self._st.set(f"เกิดข้อผิดพลาด: {e}", "err")
            messagebox.showerror("ข้อผิดพลาด", str(e))

        self._run(work, done, err)


# ═════════════════════════════════════════════════════════════
#  TAB 2 — Separate Duplicate Lines
# ═════════════════════════════════════════════════════════════
class SeparateTab(BaseTab):
    ACCENT = P["indigo"]

    def _build(self):
        body = tk.Frame(self, bg=P["white"], padx=24, pady=18)
        body.pack(fill="both", expand=True)

        lbl(body, "แยกเส้นที่ซ้อนทับออกจากกัน",
            f=F["title"], bg=P["white"]).pack(anchor="w")
        lbl(body, "ตรวจหา LineString ที่มีพิกัดซ้ำกัน แล้วขยับแต่ละเส้นออกตามระยะที่กำหนด",
            f=F["body"], fg=P["text_dim"], bg=P["white"]).pack(anchor="w", pady=(2, 12))

        NoteStrip(body, [
            "1.  เลือกไฟล์ KML ที่มีเส้น LineString ซ้อนทับกัน",
            "2.  เลือกโฟลเดอร์สำหรับบันทึกไฟล์ผลลัพธ์",
            "3.  กำหนดระยะห่างระหว่างเส้น  (แนะนำ 1–5 เมตร)",
            "4.  กด 'เริ่มประมวลผล' — เส้นแรกอยู่เดิม เส้นที่ซ้ำจะถูกขยับออกทีละระยะ",
        ], accent=P["indigo"], bg=P["indigo_bg"])

        rule(body, py=4)

        card = tk.Frame(body, bg=P["bg"], highlightbackground=P["border"],
                        highlightthickness=1, padx=14, pady=12)
        card.pack(fill="x", pady=(0, 10))
        lbl(card, "เลือกไฟล์", f=F["bold"], fg=P["text_mid"],
            bg=P["bg"]).pack(anchor="w", pady=(0, 6))
        self._ip = FilePick(card, "ไฟล์ KML ต้นฉบับ", "เลือกไฟล์",
                             self._si, accent=self.ACCENT, bg=P["bg"])
        self._fp = FilePick(card, "โฟลเดอร์บันทึก",   "เลือกโฟลเดอร์",
                             self._sf, accent=self.ACCENT, bg=P["bg"])

        dc = tk.Frame(body, bg=P["bg"], highlightbackground=P["border"],
                      highlightthickness=1, padx=14, pady=10)
        dc.pack(fill="x", pady=(0, 10))

        top = tk.Frame(dc, bg=P["bg"])
        top.pack(anchor="w")
        lbl(top, "ระยะห่างระหว่างเส้น :", f=F["bold"], bg=P["bg"]).pack(side="left")
        self._dv = tk.StringVar(value="2")
        tk.Entry(top, textvariable=self._dv, width=6, justify="center",
                 font=F["bold"], bg=P["white"], fg=self.ACCENT,
                 relief="solid", bd=1, insertbackground=self.ACCENT
                 ).pack(side="left", padx=(8, 6))
        lbl(top, "เมตร", f=F["body"], fg=P["text_mid"], bg=P["bg"]).pack(side="left")

        pr = tk.Frame(dc, bg=P["bg"])
        pr.pack(anchor="w", pady=(6, 0))
        lbl(pr, "ค่าที่แนะนำ :", f=F["small"], fg=P["text_dim"],
            bg=P["bg"]).pack(side="left", padx=(0, 8))
        for t, v in [("1 m", 1), ("2 m", 2), ("5 m", 5), ("10 m", 10)]:
            tk.Button(pr, text=t, command=lambda x=v: self._dv.set(str(x)),
                      font=F["small"], bg=P["bg2"], fg=self.ACCENT,
                      relief="flat", bd=0, padx=8, pady=3, cursor="hand2",
                      highlightbackground=P["border"], highlightthickness=1,
                      activebackground=P["indigo_bg"],
                      activeforeground=self.ACCENT).pack(side="left", padx=3)

        self._pb = ttk.Progressbar(body, mode="indeterminate", length=520)
        self._pb.pack(pady=(2, 0))

        self._build_action_row(body, "เริ่มประมวลผล", self._start, self._reset)

    def _si(self):
        p = filedialog.askopenfilename(filetypes=[("KML", "*.kml")])
        if p: self._ip.set(p)
    def _sf(self):
        p = filedialog.askdirectory()
        if p: self._fp.set(p, os.path.basename(p))
    def _reset(self):
        self._ip.reset(); self._fp.reset()
        self._dv.set("2")
        self._st.set("พร้อมใช้งาน", "idle")

    def _start(self):
        if not self._ip.get() or not self._fp.get():
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณาเลือกไฟล์และโฟลเดอร์")
            return
        try:
            dist = float(self._dv.get()); assert dist > 0
        except:
            messagebox.showerror("ค่าไม่ถูกต้อง", "ระยะทางต้องเป็นตัวเลขที่มากกว่า 0")
            return
        self._st.set("กำลังวิเคราะห์เส้น ...", "run")
        ip, fp, d = self._ip.get(), self._fp.get(), dist

        def work():
            return separate_duplicate_lines(ip, fp, d,
                       lambda m: self._st.set(m, "run"))
        def done(out):
            self._set_idle()
            self._st.set(f"สำเร็จ — {os.path.basename(out)}", "ok")
            messagebox.showinfo("สำเร็จ", f"ประมวลผลเสร็จสิ้น\n\nไฟล์ผลลัพธ์:\n{out}")
        def err(e):
            self._set_idle()
            self._st.set(f"เกิดข้อผิดพลาด: {e}", "err")
            messagebox.showerror("ข้อผิดพลาด", str(e))

        self._run(work, done, err)


# ═════════════════════════════════════════════════════════════
#  TAB 3 — Excel → KML
# ═════════════════════════════════════════════════════════════
class ExcelTab(BaseTab):
    ACCENT = P["teal"]

    def _build(self):
        body = tk.Frame(self, bg=P["white"], padx=24, pady=18)
        body.pack(fill="both", expand=True)

        lbl(body, "แปลง Excel เป็น KML",
            f=F["title"], bg=P["white"]).pack(anchor="w")
        lbl(body, "อ่านข้อมูลพิกัดจาก Excel แล้วสร้าง LineString จัดกลุ่มตาม ID ลงในไฟล์ KML",
            f=F["body"], fg=P["text_dim"], bg=P["white"]).pack(anchor="w", pady=(2, 12))

        NoteStrip(body, [
            "1.  เลือกไฟล์ Excel (.xlsx หรือ .xls)",
            "2.  เลือก Sheet จาก Dropdown (โหลดรายชื่ออัตโนมัติหลังเลือกไฟล์)",
            "3.  เลือกโฟลเดอร์สำหรับบันทึกไฟล์ KML",
            "4.  กด 'แปลงเป็น KML' — ไฟล์จะถูกตั้งชื่อตามชื่อไฟล์ Excel",
        ], accent=P["teal"], bg=P["teal_bg"])

        NoteStrip(body, [
            "คอลัมน์ที่จำเป็น :  id  |  ชื่อชุมสาย  |  พิกัด (lat lon)  |  ลำดับพิกัด",
        ], accent=P["warn_fg"], bg=P["warn_bg"])

        rule(body, py=4)

        card = tk.Frame(body, bg=P["bg"], highlightbackground=P["border"],
                        highlightthickness=1, padx=14, pady=12)
        card.pack(fill="x", pady=(0, 10))
        lbl(card, "เลือกไฟล์", f=F["bold"], fg=P["text_mid"],
            bg=P["bg"]).pack(anchor="w", pady=(0, 6))

        self._ep = FilePick(card, "ไฟล์ Excel",     "เลือกไฟล์",
                             self._se, accent=self.ACCENT, bg=P["bg"])

        sh_row = tk.Frame(card, bg=P["bg"])
        sh_row.pack(fill="x", pady=3)
        lbl(sh_row, "Sheet", f=F["bold"], bg=P["bg"],
            width=20, anchor="w").pack(side="left")
        self._shcb = ttk.Combobox(sh_row, state="readonly", width=28, font=F["body"])
        self._shcb.set("— กรุณาเลือกไฟล์ Excel ก่อน —")
        self._shcb.pack(side="left", padx=(0, 6))

        self._fp = FilePick(card, "โฟลเดอร์บันทึก", "เลือกโฟลเดอร์",
                             self._sf, accent=self.ACCENT, bg=P["bg"])

        self._pb = ttk.Progressbar(body, mode="indeterminate", length=520)
        self._pb.pack(pady=(2, 0))

        self._build_action_row(body, "แปลงเป็น KML", self._start, self._reset)

    def _se(self):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx *.xls")])
        if p:
            self._ep.set(p)
            try:
                sheets = pd.ExcelFile(p).sheet_names
                self._shcb["values"] = sheets; self._shcb.current(0)
            except Exception as e:
                messagebox.showerror("ข้อผิดพลาด", f"ไม่สามารถอ่านไฟล์:\n{e}")
    def _sf(self):
        p = filedialog.askdirectory()
        if p: self._fp.set(p, os.path.basename(p))
    def _reset(self):
        self._ep.reset(); self._fp.reset()
        self._shcb.set("— กรุณาเลือกไฟล์ Excel ก่อน —")
        self._shcb["values"] = []
        self._st.set("พร้อมใช้งาน", "idle")

    def _start(self):
        if not self._ep.get() or not self._fp.get():
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณาเลือกไฟล์ Excel และโฟลเดอร์")
            return
        sheet = self._shcb.get()
        if not sheet or "กรุณา" in sheet:
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณาเลือก Sheet")
            return
        self._st.set("กำลังอ่านข้อมูล ...", "run")
        ep, fp, sh = self._ep.get(), self._fp.get(), sheet

        def work():
            return excel_to_kml(ep, sh, fp,
                       lambda m: self._st.set(m, "run"))
        def done(out):
            self._set_idle()
            self._st.set(f"สำเร็จ — {os.path.basename(out)}", "ok")
            messagebox.showinfo("สำเร็จ", f"แปลงไฟล์เสร็จสิ้น\n\nไฟล์ KML:\n{out}")
        def err(e):
            self._set_idle()
            self._st.set(f"เกิดข้อผิดพลาด: {e}", "err")
            messagebox.showerror("ข้อผิดพลาด", str(e))

        self._run(work, done, err)


# ═════════════════════════════════════════════════════════════
#  TAB 4 — Missing Coords  (2 modes: Excel / Manual bracket)
# ═════════════════════════════════════════════════════════════
class MissingCoordsTab(BaseTab):
    ACCENT = P["ok"]

    def _build(self):
        body = tk.Frame(self, bg=P["white"], padx=24, pady=18)
        body.pack(fill="both", expand=True)

        lbl(body, "เพิ่มพิกัดที่ตกหล่น",
            f=F["title"], bg=P["white"]).pack(anchor="w")
        lbl(body, "เลือกวิธีป้อนพิกัด แล้วกำหนดตำแหน่งบันทึกไฟล์ KML",
            f=F["body"], fg=P["text_dim"], bg=P["white"]).pack(anchor="w", pady=(2, 8))

        # ── Mode selector ───────────────────────────────────
        self._mode = tk.StringVar(value="excel")
        mode_row = tk.Frame(body, bg=P["bg"], highlightbackground=P["border"],
                            highlightthickness=1, padx=10, pady=8)
        mode_row.pack(fill="x", pady=(0, 10))
        lbl(mode_row, "รูปแบบการป้อนพิกัด :", f=F["bold"],
            bg=P["bg"]).pack(side="left", padx=(0, 16))
        for val, txt in [("excel", "  แบบที่ 1 — จากไฟล์ Excel"),
                          ("manual", "  แบบที่ 2 — กรอกพิกัดด้วยตนเอง")]:
            tk.Radiobutton(mode_row, text=txt, variable=self._mode, value=val,
                           font=F["body"], bg=P["bg"], fg=P["text"],
                           activebackground=P["bg"], selectcolor=P["white"],
                           cursor="hand2",
                           command=self._switch_mode).pack(side="left", padx=8)

        rule(body, py=2)

        # ── Panel A : Excel mode ─────────────────────────────
        self._pnl_excel = tk.Frame(body, bg=P["white"])

        NoteStrip(self._pnl_excel, [
            "1.  เลือกไฟล์ Excel ที่มีคอลัมน์พิกัด (lat,lon) อยู่ในช่องเดียวกัน",
            "2.  เลือก Sheet จาก Dropdown",
            "3.  กำหนดตำแหน่งบันทึกไฟล์ KML แล้วกด 'สร้างไฟล์ KML'",
        ], accent=P["ok"], bg=P["teal_bg"])
        NoteStrip(self._pnl_excel, [
            "รองรับรูปแบบพิกัด:  '15.472...,102.106...'  ในช่องเดียว",
        ], accent=P["warn_fg"], bg=P["warn_bg"])

        card_e = tk.Frame(self._pnl_excel, bg=P["bg"],
                          highlightbackground=P["border"],
                          highlightthickness=1, padx=14, pady=12)
        card_e.pack(fill="x", pady=(0, 8))
        lbl(card_e, "รายการไฟล์ Excel", f=F["bold"], fg=P["text_mid"],
            bg=P["bg"]).pack(anchor="w", pady=(0, 6))

        # ── ชื่อเส้นทาง (ใส่ครั้งเดียวสำหรับทุกไฟล์) ────────
        ln_row = tk.Frame(card_e, bg=P["bg"])
        ln_row.pack(fill="x", pady=(0, 8))
        lbl(ln_row, "ชื่อเส้นทาง", f=F["bold"], bg=P["bg"],
            width=18, anchor="w").pack(side="left")
        self._line_name = tk.StringVar()
        tk.Entry(ln_row, textvariable=self._line_name, width=30,
                 font=F["body"], bg=P["white"], fg=P["text"],
                 relief="solid", bd=1,
                 insertbackground=self.ACCENT).pack(side="left", padx=(0, 6))
        lbl(ln_row, "(ถ้าไม่ระบุใช้ชื่อ 'พิกัดที่ตกหล่น')",
            f=F["small"], fg=P["text_dim"], bg=P["bg"]).pack(side="left")

        # ── ตาราง list ไฟล์ ──────────────────────────────────
        list_outer = tk.Frame(card_e, bg=P["bg"],
                              highlightbackground=P["border"],
                              highlightthickness=1)
        list_outer.pack(fill="x", pady=(0, 6))

        # header row (ไม่มีคอลัมน์ชื่อเส้นทางแล้ว)
        hdr_row = tk.Frame(list_outer, bg=P["bg2"])
        hdr_row.pack(fill="x")
        for txt, w in [("ไฟล์ Excel", 28), ("Sheet", 14), ("", 5)]:
            lbl(hdr_row, txt, f=F["bold"], fg=P["text_mid"],
                bg=P["bg2"], width=w, anchor="w").pack(side="left", padx=4, pady=3)

        # scrollable area
        self._file_canvas = tk.Canvas(list_outer, bg=P["bg"],
                                      height=110, highlightthickness=0)
        sb = ttk.Scrollbar(list_outer, orient="vertical",
                           command=self._file_canvas.yview)
        self._file_canvas.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._file_canvas.pack(side="left", fill="x", expand=True)

        self._file_rows_frame = tk.Frame(self._file_canvas, bg=P["bg"])
        self._file_canvas.create_window((0, 0), window=self._file_rows_frame,
                                        anchor="nw")
        self._file_rows_frame.bind(
            "<Configure>",
            lambda e: self._file_canvas.configure(
                scrollregion=self._file_canvas.bbox("all")))

        self._file_entries = []   # list of dicts {path, sheet_var, shcb, row_frame}

        # add/remove buttons
        btn_row = tk.Frame(card_e, bg=P["bg"])
        btn_row.pack(fill="x", pady=(0, 6))
        tk.Button(btn_row, text="＋  เพิ่มไฟล์ Excel", command=self._add_excel_files,
                  font=F["btn"], bg=self.ACCENT, fg=P["white"],
                  relief="flat", bd=0, padx=12, pady=5, cursor="hand2",
                  activebackground=P["teal_bg"],
                  activeforeground=self.ACCENT).pack(side="left")
        tk.Button(btn_row, text="✕  ลบทั้งหมด", command=self._clear_excel_files,
                  font=F["btn"], bg=P["white"], fg=P["stop"],
                  relief="flat", bd=0, padx=12, pady=5, cursor="hand2",
                  highlightbackground=P["border"], highlightthickness=1,
                  activebackground=P["stop_bg"],
                  activeforeground=P["stop"]).pack(side="left", padx=8)

        # ── ตัวเลือกแสดงหมุด ────────────────────────────────
        self._show_pts = tk.BooleanVar(value=True)
        chk_row = tk.Frame(card_e, bg=P["bg"])
        chk_row.pack(fill="x", pady=(2, 2))
        tk.Checkbutton(chk_row, text="แสดงหมุด (Point) แต่ละจุด",
                       variable=self._show_pts,
                       font=F["body"], bg=P["bg"], fg=P["text"],
                       activebackground=P["bg"], selectcolor=P["white"],
                       cursor="hand2").pack(side="left")
        lbl(chk_row, "  — ชื่อหมุดจะแสดงเป็นค่า Tag จาก Excel",
            f=F["small"], fg=P["text_dim"], bg=P["bg"]).pack(side="left")

        self._op_e = FilePick(card_e, "บันทึกผลลัพธ์เป็น", "กำหนดตำแหน่ง",
                               self._so_e, accent=self.ACCENT, bg=P["bg"])

        # ── Panel B : Manual bracket mode ───────────────────
        self._pnl_manual = tk.Frame(body, bg=P["white"])

        NoteStrip(self._pnl_manual, [
            "1.  วางพิกัดในรูปแบบ  (lat,lon)(lat,lon)...",
            "2.  กรอกรายละเอียด (Description) ถ้าต้องการ — ไม่บังคับ",
            "3.  กำหนดตำแหน่งบันทึกไฟล์ KML แล้วกด 'สร้างไฟล์ KML'",
        ], accent=P["ok"], bg=P["teal_bg"])
        NoteStrip(self._pnl_manual, [
            "ตัวอย่าง:  (15.4725,102.1065)(15.4724,102.1063)(15.4722,102.1060)",
        ], accent=P["warn_fg"], bg=P["warn_bg"])

        card_m = tk.Frame(self._pnl_manual, bg=P["bg"],
                          highlightbackground=P["border"],
                          highlightthickness=1, padx=14, pady=12)
        card_m.pack(fill="x", pady=(0, 8))

        lbl(card_m, "พิกัด (วงเล็บ)", f=F["bold"], fg=P["text_mid"],
            bg=P["bg"]).pack(anchor="w", pady=(0, 4))
        self._coord_txt = tk.Text(card_m, height=4, font=F["body"],
                                  bg=P["white"], fg=P["text"],
                                  relief="solid", bd=1,
                                  insertbackground=self.ACCENT,
                                  wrap="word")
        self._coord_txt.pack(fill="x", pady=(0, 8))

        # Description fields (collapsible via toggle)
        self._desc_open = tk.BooleanVar(value=False)
        tog_row = tk.Frame(card_m, bg=P["bg"])
        tog_row.pack(fill="x")
        self._tog_btn = tk.Button(
            tog_row, text="▶  รายละเอียด (Description) — คลิกเพื่อเพิ่ม",
            font=F["bold"], bg=P["bg"], fg=P["text_mid"],
            relief="flat", bd=0, padx=0, pady=4, cursor="hand2",
            anchor="w", activebackground=P["bg"],
            activeforeground=P["text"],
            command=self._toggle_desc)
        self._tog_btn.pack(fill="x")

        self._desc_frame = tk.Frame(card_m, bg=P["bg"])
        self._desc_vars  = {}
        col_frames = [tk.Frame(self._desc_frame, bg=P["bg"]),
                      tk.Frame(self._desc_frame, bg=P["bg"])]
        col_frames[0].pack(side="left", fill="both", expand=True, padx=(0, 8))
        col_frames[1].pack(side="left", fill="both", expand=True)
        for i, h in enumerate(_DESC_HEADERS):
            var = tk.StringVar()
            self._desc_vars[h] = var
            parent = col_frames[i % 2]
            lbl(parent, h, f=F["small"], fg=P["text_mid"],
                bg=P["bg"]).pack(anchor="w", pady=(4, 0))
            tk.Entry(parent, textvariable=var, font=F["body"],
                     bg=P["white"], fg=P["text"], relief="solid", bd=1,
                     insertbackground=self.ACCENT
                     ).pack(fill="x")

        self._op_m = FilePick(card_m, "บันทึกผลลัพธ์เป็น", "กำหนดตำแหน่ง",
                               self._so_m, accent=self.ACCENT, bg=P["bg"])

        # ── ตัวเลือกแสดงหมุด (manual mode) ─────────────────
        self._show_pts_m = tk.BooleanVar(value=False)
        chk_m = tk.Frame(card_m, bg=P["bg"])
        chk_m.pack(fill="x", pady=(4, 0))
        tk.Checkbutton(chk_m, text="แสดงหมุด (Point) แต่ละจุด",
                       variable=self._show_pts_m,
                       font=F["body"], bg=P["bg"], fg=P["text"],
                       activebackground=P["bg"], selectcolor=P["white"],
                       cursor="hand2").pack(side="left")

        # ── Shared: progressbar + action row ────────────────
        self._pb = ttk.Progressbar(body, mode="indeterminate", length=520)
        self._pb.pack(pady=(4, 0))
        self._build_action_row(body, "สร้างไฟล์ KML", self._start, self._reset)

        # show initial panel
        self._switch_mode()

    # ── mode switch ──────────────────────────────────────────
    def _switch_mode(self):
        if self._mode.get() == "excel":
            self._pnl_manual.pack_forget()
            self._pnl_excel.pack(fill="x", before=self._pb)
        else:
            self._pnl_excel.pack_forget()
            self._pnl_manual.pack(fill="x", before=self._pb)

    # ── description toggle ───────────────────────────────────
    def _toggle_desc(self):
        if self._desc_open.get():
            self._desc_frame.pack_forget()
            self._desc_open.set(False)
            self._tog_btn.config(
                text="▶  รายละเอียด (Description) — คลิกเพื่อเพิ่ม")
        else:
            self._desc_frame.pack(fill="x", pady=(6, 0))
            self._desc_open.set(True)
            self._tog_btn.config(
                text="▼  รายละเอียด (Description) — คลิกเพื่อซ่อน")

    # ── multi-file helpers ───────────────────────────────────
    def _add_excel_files(self):
        paths = filedialog.askopenfilenames(
            filetypes=[("Excel", "*.xlsx *.xls")])
        for p in paths:
            self._add_file_row(p)

    def _add_file_row(self, path):
        """เพิ่มแถวไฟล์ใหม่ใน list"""
        entry = {"path": path, "sheet_var": tk.StringVar()}

        row = tk.Frame(self._file_rows_frame, bg=P["bg"],
                       highlightbackground=P["border"], highlightthickness=1)
        row.pack(fill="x", pady=1, padx=1)
        entry["row_frame"] = row

        # ชื่อไฟล์
        fname = os.path.basename(path)
        short = fname[:34] + "…" if len(fname) > 36 else fname
        lbl(row, short, f=F["small"], fg=P["text"], bg=P["bg"],
            width=28, anchor="w").pack(side="left", padx=(6, 2), pady=4)

        # Sheet combobox
        shcb = ttk.Combobox(row, textvariable=entry["sheet_var"],
                             state="readonly", width=12, font=F["small"])
        try:
            sheets = pd.ExcelFile(path).sheet_names
            shcb["values"] = sheets
            entry["sheet_var"].set(sheets[0])
        except Exception:
            shcb["values"] = []
            entry["sheet_var"].set("?")
        shcb.pack(side="left", padx=4, pady=4)
        entry["shcb"] = shcb

        # ปุ่มลบแถว
        tk.Button(row, text="✕",
                  command=lambda r=row, e=entry: self._remove_file_row(r, e),
                  font=F["small"], bg=P["stop_bg"], fg=P["stop"],
                  relief="flat", bd=0, padx=6, pady=2, cursor="hand2",
                  activebackground="#fee2e2").pack(side="left", padx=(2, 4))

        self._file_entries.append(entry)
        self._file_canvas.update_idletasks()
        self._file_canvas.configure(
            scrollregion=self._file_canvas.bbox("all"))

    def _remove_file_row(self, row_frame, entry):
        row_frame.destroy()
        if entry in self._file_entries:
            self._file_entries.remove(entry)

    def _clear_excel_files(self):
        for e in list(self._file_entries):
            e["row_frame"].destroy()
        self._file_entries.clear()

    def _so_e(self):
        p = filedialog.asksaveasfilename(defaultextension=".kml",
                                          filetypes=[("KML", "*.kml")])
        if p: self._op_e.set(p)

    def _so_m(self):
        p = filedialog.asksaveasfilename(defaultextension=".kml",
                                          filetypes=[("KML", "*.kml")])
        if p: self._op_m.set(p)

    # ── reset ────────────────────────────────────────────────
    def _reset(self):
        self._clear_excel_files()
        self._op_e.reset(); self._op_m.reset()
        self._line_name.set("")
        self._show_pts.set(True)
        self._show_pts_m.set(False)
        self._coord_txt.delete("1.0", "end")
        for v in self._desc_vars.values(): v.set("")
        self._st.set("พร้อมใช้งาน", "idle")

    # ── start ─────────────────────────────────────────────────
    def _start(self):
        if self._mode.get() == "excel":
            self._start_excel()
        else:
            self._start_manual()

    def _start_excel(self):
        if not self._file_entries:
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณาเพิ่มไฟล์ Excel อย่างน้อย 1 ไฟล์")
            return
        if not self._op_e.get():
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณากำหนดตำแหน่งบันทึกไฟล์ KML")
            return

        paths  = [e["path"]           for e in self._file_entries]
        sheets = [e["sheet_var"].get() for e in self._file_entries]
        op     = self._op_e.get()
        ln     = self._line_name.get()
        sp     = self._show_pts.get()

        self._st.set("กำลังประมวลผล ...", "run")

        def work():
            return missing_coords_excel_to_kml(
                paths, sheets, ln, op,
                show_points=sp,
                cb=lambda m: self._st.set(m, "run"))

        def done(result):
            out, errors = result
            self._set_idle()
            if errors:
                warn = "\n".join(errors)
                self._st.set(f"เสร็จสิ้น (มีบางไฟล์ผิดพลาด)", "warn")
                messagebox.showwarning("เสร็จสิ้นพร้อมคำเตือน",
                    f"สร้างไฟล์ KML เสร็จ แต่มีข้อผิดพลาด:\n{warn}\n\nบันทึกที่:\n{out}")
            else:
                self._st.set(f"สำเร็จ — {os.path.basename(out)}", "ok")
                messagebox.showinfo("สำเร็จ",
                    f"สร้างไฟล์ KML เสร็จสิ้น ({len(paths)} ไฟล์)\n\nบันทึกที่:\n{out}")

        def err(e):
            self._set_idle()
            self._st.set(f"เกิดข้อผิดพลาด: {e}", "err")
            messagebox.showerror("ข้อผิดพลาด", str(e))

        self._run(work, done, err)

    def _start_manual(self):
        coord_str = self._coord_txt.get("1.0", "end").strip()
        if not coord_str:
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณาใส่พิกัดอย่างน้อย 1 จุด")
            return
        if not self._op_m.get():
            messagebox.showerror("ข้อมูลไม่ครบ", "กรุณากำหนดตำแหน่งบันทึกไฟล์")
            return
        desc = {h: v.get() for h, v in self._desc_vars.items() if v.get().strip()}
        sp   = self._show_pts_m.get()
        self._st.set("กำลังประมวลผล ...", "run")
        cs, op, dc = coord_str, self._op_m.get(), desc

        def work():
            return manual_coords_to_kml(cs, op, dc,
                       show_points=sp,
                       cb=lambda m: self._st.set(m, "run"))
        def done(out):
            self._set_idle()
            self._st.set(f"สำเร็จ — {os.path.basename(out)}", "ok")
            messagebox.showinfo("สำเร็จ", f"สร้างไฟล์ KML เสร็จสิ้น\n\nบันทึกที่:\n{out}")
        def err(e):
            self._set_idle()
            self._st.set(f"เกิดข้อผิดพลาด: {e}", "err")
            messagebox.showerror("ข้อผิดพลาด", str(e))
        self._run(work, done, err)


# ═════════════════════════════════════════════════════════════
#  APPLICATION SHELL
# ═════════════════════════════════════════════════════════════
class App(tk.Tk):
    _DEFS = [
        ("ตัด & รวม KML",        ClipTab,          P["blue"]),
        ("แยกเส้นซ้อนทับ",       SeparateTab,      P["indigo"]),
        ("Excel  →  KML",        ExcelTab,         P["teal"]),
        ("พิกัดตกหล่น",  MissingCoordsTab, P["ok"]),
    ]

    def __init__(self):
        super().__init__()
        self.title("KML Tools Suite")
        self.resizable(False, False)
        self.configure(bg=P["white"])
        self._active = -1
        self._pages  = []
        self._build()
        self.update_idletasks()

    def _build(self):
        hdr = tk.Frame(self, bg=P["white"], highlightbackground=P["border"],
                       highlightthickness=1, height=46)
        hdr.pack(fill="x"); hdr.pack_propagate(False)

        left = tk.Frame(hdr, bg=P["white"])
        left.pack(side="left", fill="y", padx=22)
        lbl(left, "KML Tools Suite", f=F["app"], bg=P["white"]).pack(side="left", pady=13)
        tk.Frame(left, bg=P["border"], width=1).pack(side="left", fill="y", pady=10, padx=12)
        lbl(left, "เครื่องมือจัดการไฟล์ KML",
            f=F["body"], fg=P["text_dim"], bg=P["white"]).pack(side="left", pady=13)
        tk.Label(hdr, text=" v2.0 ", font=F["small"], bg=P["bg"], fg=P["text_mid"],
                 highlightbackground=P["border"], highlightthickness=1
                 ).pack(side="right", padx=20, pady=14)

        nav = tk.Frame(self, bg=P["bg"], highlightbackground=P["border"],
                       highlightthickness=1, height=38)
        nav.pack(fill="x"); nav.pack_propagate(False)

        self._btns = []; self._inds = []
        for i, (name, cls, accent) in enumerate(self._DEFS):
            col = tk.Frame(nav, bg=P["bg"]); col.pack(side="left")
            btn = tk.Button(col, text=f"  {name}  ", font=F["nav"],
                            bg=P["bg"], fg=P["text_dim"], relief="flat", bd=0,
                            padx=10, pady=8, cursor="hand2",
                            activebackground=P["white"], activeforeground=P["text"],
                            command=lambda i=i: self._switch(i))
            btn.pack()
            ind = tk.Frame(col, height=2, bg=P["bg"]); ind.pack(fill="x")
            ind._accent = accent
            self._btns.append(btn); self._inds.append(ind)

        tk.Frame(self, bg=P["border"], height=1).pack(fill="x")

        self._cont = tk.Frame(self, bg=P["white"])
        self._cont.pack(fill="both", expand=True)
        for _, cls, _ in self._DEFS:
            self._pages.append(cls(self._cont))
        self._switch(0)

    def _switch(self, idx):
        if idx == self._active: return
        for i, (pg, btn, ind) in enumerate(zip(self._pages, self._btns, self._inds)):
            if i == idx:
                pg.pack(fill="both", expand=True)
                btn.config(fg=P["text"], bg=P["white"], font=F["nav_act"])
                ind.config(bg=ind._accent)
            else:
                pg.pack_forget()
                btn.config(fg=P["text_dim"], bg=P["bg"], font=F["nav"])
                ind.config(bg=P["bg"])
        self._active = idx


# ═════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = App()
    s = ttk.Style(app)
    s.theme_use("clam")
    s.configure("TProgressbar",
                troughcolor=P["bg"], background=P["blue"],
                bordercolor=P["border"], lightcolor=P["blue"],
                darkcolor=P["blue"], thickness=3)
    s.configure("TCombobox",
                fieldbackground=P["white"], background=P["white"],
                foreground=P["text"], arrowcolor=P["text_mid"],
                selectbackground=P["blue_bg"], selectforeground=P["blue"])
    s.map("TCombobox",
          fieldbackground=[("readonly", P["white"])],
          foreground=[("readonly", P["text"])])
    app.mainloop()