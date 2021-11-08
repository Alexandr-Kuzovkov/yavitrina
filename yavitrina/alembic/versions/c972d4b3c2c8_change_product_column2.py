"""change product column2

Revision ID: c972d4b3c2c8
Revises: 7ca37914a218
Create Date: 2021-11-08 16:51:25.529491

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c972d4b3c2c8'
down_revision = '446a371eafed'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('UPDATE product SET feedbacks=NULL WHERE feedbacks NOTNULL')
    op.execute('ALTER TABLE product ALTER COLUMN feedbacks TYPE jsonb USING feedbacks::jsonb')


def downgrade():
    pass
