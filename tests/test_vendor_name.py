from app.ingestion import adapt_marketcheck_item, derive_vendor_name


def test_derive_vendor_name_prefers_explicit_dealer_name() -> None:
    item = {"dealer_name": "Local EV House", "source": "carvana.com"}
    assert derive_vendor_name(item, "https://carvana.com/vehicle/123") == "Local EV House"


def test_derive_vendor_name_from_source_domain() -> None:
    item = {"source": "carvana.com"}
    assert derive_vendor_name(item, None) == "Carvana"


def test_derive_vendor_name_from_url_when_other_fields_missing() -> None:
    item = {}
    assert (
        derive_vendor_name(item, "https://www.herbchambersinfinitiofwestborough.com/inventory/abc")
        == "Herbchambersinfinitiofwestborough"
    )


def test_adapt_marketcheck_item_uses_url_fallback_for_dealer_name() -> None:
    adapted = adapt_marketcheck_item(
        {
            "model": "Model 3",
            "build": {"year": 2024},
            "vdp_url": "https://www.carvana.com/vehicle/4107149",
            "source": "mc",
            "heading": "2024 Tesla Model 3",
            "price": 35000,
            "city": "Framingham",
            "state": "MA",
        }
    )
    assert adapted["dealer_name"] == "Carvana"

