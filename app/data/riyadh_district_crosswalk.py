"""Riyadh district AR -> EN crosswalk.

Source of truth for the Arabic-to-English district name mapping used
across Oaktree Atlas. Consumed by:

  * ``app/ingest/aqar_district_hulls.py`` — populates
    ``external_feature.properties.district_en`` at insert time so the
    ``aqar_district_hulls`` layer carries both the Arabic norm key and
    its conventional English transliteration.
  * (eventually) the EN -> AR resolver in
    ``app/services/expansion_advisor.py`` — lets a user-supplied English
    label round-trip back to the canonical Arabic norm key used by the
    rent-comp join and the candidate-pool filters.

Coverage: 152 entries — covers all 146 distinct Arabic district keys
present in ``external_feature WHERE layer_name='aqar_district_hulls'``
as of 2026-04-27, plus 6 historical/fallback variants (``البطحاء``,
``التخصصي``, ``الشفاء``, ``المؤنسية``, ``المنسية``, ``النسيم``) that do
not appear in the current hull dump but are well-known Riyadh district
names worth resolving if they show up later.

Key conventions
---------------
Keys are in ``normalize_district_key``-form
(``app/services/aqar_district_match.py``):

  * No leading ``"حي "`` prefix.
  * Tatweel (``\\u0640``) and bidi/zero-width controls stripped.
  * Alef variants normalized: ``أ``, ``إ``, ``آ`` -> ``ا``.
  * Ya normalized: ``ى`` -> ``ي``.
  * Whitespace collapsed and trimmed.

Lookup contract: for any raw district label ``raw`` that originates
from the same district, ``normalize_district_key(raw)`` must equal
exactly one key in ``RIYADH_DISTRICT_AR_TO_EN``. This is why e.g.
``الأندلس`` is stored as ``الاندلس`` (alef-flipped), ``الخزامى`` as
``الخزامي`` (ya-flipped), and ``العريجاء الوسطى`` as
``العريجاء الوسطي``.

Word order matches DB storage order (noun-then-adjective in Arabic):
``السويدي الغربي``, ``العريجاء الغربية``, ``العريجاء الوسطي``,
``ظهرة لبن``. The English transliteration applies the conventional
word-order flips (``Dhahrat X``, ``Umm X``, ``Dahyat X``,
``Princess X University``).

English values follow conventional Saudi-address Romanization:

  * ``"Al "`` definite article (no hyphen, no ``"El "``).
  * Sun-letter assimilation: ``"As "`` (س ص), ``"Ash "`` (ش),
    ``"Ad "`` (د ض), ``"Ar "`` (ر), ``"Az "`` (ز),
    ``"At "`` (ت ط), ``"An "`` (ن). Pool districts that the wider
    repo writes with a plain ``"Al "`` even though the initial Arabic
    letter is a sun letter (e.g. ``"Al Nakheel"``) keep that
    established form for compatibility.
  * Word-order flips for compounds: ``"X ظهرة"`` -> ``"Dhahrat X"``,
    ``"X ضاحية"`` -> ``"Dahyat X"``, ``"X ام"`` -> ``"Umm X"``,
    ``"X الأميرة جامعة"`` -> ``"Princess X University"``.

Entries with a ``# VERIFY:`` comment are low-confidence transliterations
or rows that look like roads/landmarks/institutions rather than
residential districts; review with a stakeholder before treating them
as authoritative.
"""

from __future__ import annotations

# Keys are sorted by Arabic key in Unicode code-point order, which for
# the Arabic alphabet block (U+0627..U+064A) approximates the
# conventional Arabic alphabetical order Postgres uses with the default
# ar_SA collation. Keep this dict alphabetically sorted by key — the
# verification SQL diff is much easier to read when the order is stable.
RIYADH_DISTRICT_AR_TO_EN: dict[str, str] = {
    "احد": "Uhud",  # VERIFY: short bare name; could be a street/area rather than a residential district
    "اشبيلية": "Ishbiliyah",  # POOL — also covers source variant ``إشبيلية`` (alef-flipped to اشبيلية by normalize_district_key)
    "الازدهار": "Al Izdihar",
    "الاندلس": "Al Andalus",  # source form ``الأندلس``; ``أ`` -> ``ا`` after norm
    "البديعة": "Al Badiah",
    "البرية": "Al Bariyah",
    "البطحاء": "Al Batha",  # NOT in current aqar_district_hulls dump (2026-04-27); kept as historical/fallback
    "التخصصي": "Al Takhassusi",  # NOT in current aqar_district_hulls dump (2026-04-27); kept as historical/fallback
    "التعاون": "At Taawun",
    "الجرادية": "Al Jaradiyah",
    "الجزيرة": "Al Jazirah",
    "الجنادرية": "Al Janadriyah",  # POOL
    "الحائر": "Al Hayer",
    "الحزم": "Al Hazm",
    "الحمراء": "Al Hamra",
    "الخالدية": "Al Khalidiyah",
    "الخزامي": "Al Khuzama",  # source form ``الخزامى``; ``ى`` -> ``ي`` after norm
    "الخليج": "Al Khaleej",  # POOL
    "الدار البيضاء": "Al Dar Al Baida",
    "الدريهمية": "Ad Duraihimiyah",  # VERIFY: unusual diminutive Romanization
    "الديرة": "Ad Dirah",
    "الرائد": "Ar Raed",
    "الربوة": "Al Rabwah",
    "الربيع": "Ar Rabi",
    "الرحمانية": "Ar Rahmaniyah",
    "الرفيعة": "Ar Rafiah",
    "الرمال": "Ar Rimal",  # POOL
    "الروابي": "Ar Rawabi",
    "الروضة": "Al Rawdah",
    "الريان": "Ar Rayyan",
    "الزهراء": "Az Zahra",
    "الزهرة": "Az Zahrah",
    "السعادة": "As Saadah",
    "السلام": "As Salam",
    "السلي": "As Sulay",  # POOL
    "السليمانية": "As Sulimaniyah",  # POOL
    "السويدي": "Al Suwaidi",  # bare form; the western variant ``السويدي الغربي`` is a separate POOL entry
    "السويدي الغربي": "As Suwaidi Al Gharbi",  # POOL — DB word order (noun-adjective)
    "الشرفية": "Ash Sharafiyah",
    "الشرق": "Ash Sharq",
    "الشفا": "Ash Shifa",  # DB form (no ء)
    "الشفاء": "Al Shifa",  # fallback for the variant spelling with ء; NOT in current DB dump
    "الشميسي": "Ash Shumaisi",
    "الشهداء": "Al Shuhada",
    "الصالحية": "As Salihiyah",
    "الصحافة": "As Sahafah",  # sun letter ص
    "الصفا": "As Safa",
    "الصناعية": "As Sinaiyah",
    "الضباط": "Ad Dubbat",
    "العارض": "Al Arid",  # POOL
    "العريجاء": "Al Uraija",
    "العريجاء الغربية": "Al Uraija Al Gharbiyah",  # POOL — DB word order (noun-adjective)
    "العريجاء الوسطي": "Al Uraija Al Wusta",  # POOL — DB word order; raw form ``العريجاء الوسطى`` (ى -> ي after norm)
    "العزيزية": "Al Aziziyah",  # POOL
    "العقيق": "Al Aqiq",  # POOL
    "العليا": "Al Olaya",  # POOL
    "العمل": "Al Amal",
    "العود": "Al Oud",
    "الغدير": "Al Ghadir",
    "الغنامية": "Al Ghannamiyah",
    "الفاخرية": "Al Fakhriyah",
    "الفاروق": "Al Farouq",
    "الفلاح": "Al Falah",
    "الفيحاء": "Al Fayha",
    "الفيصلية": "Al Faisaliyah",  # POOL
    "القادسية": "Al Qadisiyah",
    "القدس": "Al Quds",
    "القيروان": "Al Qayrawan",
    "المؤتمرات": "Al Mutamarat",
    "المؤنسية": "Al Munsiyah",  # POOL — variant with ؤ; see also المنسية and المونسية below; all map to the same English label
    "المحمدية": "Al Muhammadiyah",
    "المدينة الصناعية الجديدة": "Al Madinah As Sinaiyah Al Jadidah",  # VERIFY: long compound; sometimes rendered "New Industrial City"
    "المربع": "Al Murabba",  # POOL
    "المرسلات": "Al Mursalat",  # POOL
    "المروة": "Al Marwah",
    "المروج": "Al Murooj",
    "المشاعل": "Al Mashail",
    "المصانع": "Al Masani",
    "المصفاة": "Al Masfah",
    "المصيف": "Al Masif",
    "المعذر الشمالي": "Al Maazar Ash Shamali",  # VERIFY: المعذر also seen as Al Maather/Al Muathar; sun letter ش in qualifier
    "المعيزلة": "Al Muaizilah",
    "المغرزات": "Al Mughrazat",
    "الملز": "Al Malaz",
    "الملقا": "Al Malqa",
    "الملك عبدالعزيز": "King Abdulaziz",  # VERIFY: looks like a road/landmark, not a residential district — confirm with stakeholder
    "الملك عبدالله": "King Abdullah",  # VERIFY: looks like a road/landmark, not a residential district — confirm with stakeholder
    "الملك فهد": "King Fahd",  # VERIFY: looks like a road/landmark, not a residential district — confirm with stakeholder
    "الملك فيصل": "King Faisal",  # VERIFY: looks like a road/landmark, not a residential district — confirm with stakeholder
    "المناخ": "Al Manakh",
    "المنار": "Al Manar",  # POOL
    "المنسية": "Al Munsiyah",  # variant of المؤنسية found in RIYADH_DISTRICTS source data; NOT in current DB dump but kept as fallback
    "المنصورة": "Al Mansurah",
    "المنصورية": "Al Mansuriyah",
    "المهدية": "Al Mahdiyah",  # POOL
    "المونسية": "Al Munsiyah",  # variant of المؤنسية with و in place of ؤ; appears in DB dump
    "الناصرية": "An Nasiriyah",
    "النخيل": "Al Nakheel",  # POOL
    "الندي": "An Nada",  # source form ``الندى``; ``ى`` -> ``ي`` after norm
    "النرجس": "An Narjis",  # POOL
    "النزهة": "An Nuzhah",
    "النسيم": "Al Naseem",  # bare fallback; NOT in DB dump (DB only carries النسيم الشرقي and النسيم الغربي)
    "النسيم الشرقي": "An Naseem Ash Sharqi",
    "النسيم الغربي": "An Naseem Al Gharbi",
    "النظيم": "An Nadhim",  # VERIFY: also Romanized as "An Nazim"
    "النفل": "An Nafal",
    "النموذجية": "An Namudhajiyah",
    "النهضة": "An Nahdah",
    "النور": "An Nur",
    "الهدا": "Al Hada",
    "الواحة": "Al Wahah",
    "الوادي": "Al Wadi",  # POOL
    "الورود": "Al Wurud",  # POOL
    "الوزارات": "Al Wizarat",
    "الوشام": "Al Wisham",
    "الياسمين": "Al Yasmin",  # POOL
    "اليرموك": "Al Yarmouk",
    "اليمامة": "Al Yamamah",
    "ام الحمام الشرقي": "Umm Al Hamam Ash Sharqi",
    "ام الحمام الغربي": "Umm Al Hamam Al Gharbi",
    "ام الشعال": "Umm Ash Shaal",
    "ام سليم": "Umm Salim",
    "بدر": "Badr",
    "بنبان": "Banban",
    "ثليم": "Thulaim",  # VERIFY: rare bare name
    "جامعة الاميرة نورة": "Princess Nourah University",  # VERIFY: an institution/campus, not a residential district; raw form ``جامعة الأميرة نورة`` (أ -> ا after norm)
    "جرير": "Jarir",
    "حطين": "Hittin",  # POOL
    "ديراب": "Dirab",
    "سلطانة": "Sultanah",
    "شبرا": "Shubra",
    "صلاح الدين": "Salah Ad Din",  # second word's article assimilates with sun letter د
    "ضاحية نمار": "Dahyat Namar",
    "طويق": "Tuwaiq",  # POOL
    "طيبة": "Taybah",
    "ظهرة البديعة": "Dhahrat Al Badiah",
    "ظهرة لبن": "Dhahrat Laban",  # POOL — DB word order (Dhahrat first)
    "ظهرة نمار": "Dhahrat Namar",
    "عتيقة": "Atiqah",
    "عرقة": "Irqah",
    "عريض": "Uraid",  # VERIFY: rare bare name; easily confused with العارض (Al Arid)
    "عكاظ": "Ukaz",
    "عليشة": "Ulaysha",
    "غبيرة": "Ghubaira",
    "غرناطة": "Ghirnatah",
    "قرطبة": "Qurtubah",
    "لبن": "Laban",
    "مطار الملك خالد الدولي": "King Khalid International Airport",  # VERIFY: airport facility, not a residential district — confirm whether it should appear in the candidate pool
    "منفوحة": "Manfuhah",  # POOL
    "منفوحة الجديدة": "Manfuhah Al Jadidah",
    "نمار": "Namar",
    "هيت": "Hit",  # VERIFY: rare bare name
}


"""How to maintain
---------------
This crosswalk is the single point of truth for AR -> EN district
names. As of 2026-04-27 it covers all 146 distinct keys present in
``external_feature WHERE layer_name='aqar_district_hulls'`` (plus a
small number of historical/fallback variants kept for resilience).

When ``app/ingest/aqar_district_hulls.py`` ingests a new hull whose
``properties.district`` (after ``normalize_district_key``) is not
present here, the loader logs an ``unmapped district key`` warning
and falls back to leaving ``district_en`` NULL for that row. To
resolve: add a new entry to ``RIYADH_DISTRICT_AR_TO_EN`` with the
Arabic key in ``normalize_district_key``-form (no حي prefix, alef
and ya variants normalized) and its conventional English
transliteration, keep the dict alphabetically sorted by Arabic key,
and re-run the loader.

If the same district appears under multiple Arabic shapes that
survive normalization (e.g. ؤ vs و vs no hamza, or ء present vs
absent), add each variant as its own key mapping to the same English
value and leave a one-line comment on the variant rows pointing back
to the canonical form.

Use ``# VERIFY: <reason>`` on any new entry where the transliteration
is uncertain, the Arabic shape is unusual, or the row looks like a
road / landmark / institution rather than a residential district.
The integrity test (see ``tests/test_riyadh_district_crosswalk.py``
once added, or the inline check in CI) must keep passing:

    from app.data.riyadh_district_crosswalk import RIYADH_DISTRICT_AR_TO_EN
    from app.services.aqar_district_match import normalize_district_key
    for k in RIYADH_DISTRICT_AR_TO_EN:
        assert normalize_district_key(k) == k, k
"""
