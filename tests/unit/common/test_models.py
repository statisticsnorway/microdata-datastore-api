import pytest
from datastore_api.common.models import Version


def test_version():
    version = Version.from_str("1.0.0.0")
    assert version.major == "1"
    assert version.minor == "0"
    assert version.patch == "0"
    assert version.draft == "0"

    assert version.to_2_underscored() == "1_0"
    assert version.to_3_underscored() == "1_0_0"
    assert version.to_4_dotted() == "1.0.0.0"

    assert str(version) == "1.0.0.0"

    with pytest.raises(ValueError) as e:
        Version.from_str("")
    assert "Incorrect version format" in str(e)

    with pytest.raises(ValueError) as e:
        Version.from_str("1.2.3.*")
    assert "Incorrect version format" in str(e)

    with pytest.raises(ValueError) as e:
        Version.from_str("1.2.3.4.5")
    assert "Incorrect version format" in str(e)
