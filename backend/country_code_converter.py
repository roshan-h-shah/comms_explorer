'''
Goes from country to country code and vice versa (certain websites need the Alpha-2 country code instead of the name of the country)
'''
import pycountry
# --- Country Name to ISO 3166-1 Alpha-2 Code ---
def get_alpha2_from_country_name(country_name):
    try:
        # Search by common name or official name
        country = pycountry.countries.get(name=country_name)
        if country:
            return country.alpha_2
        # Try searching by common_name if direct name fails for some cases
        for c in pycountry.countries:
            if hasattr(c, 'common_name') and c.common_name == country_name:
                return c.alpha_2
        # Some countries might have an official_name that differs
        for c in pycountry.countries:
            if hasattr(c, 'official_name') and c.official_name == country_name:
                return c.alpha_2
        return None # Not found
    except KeyError:
        return None
    except AttributeError: # Some entries might not have 'common_name' or 'official_name'
        return None

# --- ISO 3166-1 Alpha-2 Code to Country Name ---
def get_country_name_from_alpha2(alpha2_code):
    try:
        country = pycountry.countries.get(alpha_2=alpha2_code)
        if country:
            return country.name # This gives the standard name
        return None
    except KeyError:
        return None

