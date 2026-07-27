from django.contrib.auth import get_user_model

from georiva.organisations.models import OrganisationMembership

PASSWORD = "org-foundation-test-pw"


def make_user(username, *, superuser=False, password=PASSWORD, **kwargs):
    User = get_user_model()
    factory = User.objects.create_superuser if superuser else User.objects.create_user
    return factory(username=username, email=f"{username}@example.test", password=password, **kwargs)


def add_member(user, organisation, role=OrganisationMembership.Role.MEMBER):
    return OrganisationMembership.objects.create(user=user, organisation=organisation, role=role)
