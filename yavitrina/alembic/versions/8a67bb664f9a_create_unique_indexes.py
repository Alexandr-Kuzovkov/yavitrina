"""create_unique_indexes

Revision ID: 8a67bb664f9a
Revises: 50e5e298dc10
Create Date: 2021-12-02 18:22:24.053763

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8a67bb664f9a'
down_revision = '50e5e298dc10'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('CREATE UNIQUE INDEX uidx_settings_url_name on settings (url,"name")')
    op.execute('CREATE UNIQUE INDEX uidx_settings_value_setting_name_value on settings_value (settings_name,"value")')


def downgrade():
    pass
