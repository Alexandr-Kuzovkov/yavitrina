"""create image index

Revision ID: 71125e4ca796
Revises: f42bc6a3acec
Create Date: 2021-10-08 16:05:31.608799

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '71125e4ca796'
down_revision = 'f42bc6a3acec'
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint("uidx_image_url", "image", ["url"])


def downgrade():
    op.drop_constraint("uidx_image_url", "image")
