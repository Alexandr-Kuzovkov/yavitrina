"""create_tables_for_settings

Revision ID: 50e5e298dc10
Revises: 07e068fbe8f7
Create Date: 2021-12-02 18:00:49.078725

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '50e5e298dc10'
down_revision = '07e068fbe8f7'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('''CREATE TABLE IF NOT EXISTS settings (
                  id bigserial not null constraint settings_pk primary key,
                  url VARCHAR(255) NOT NULL,
                  "name" VARCHAR(255) NOT NULL)''')



    op.execute('''CREATE TABLE IF NOT EXISTS settings_value (
                  id serial not null constraint settings_value_pk primary key,
                  settings_name BIGINT NOT NULL,
                  "value" VARCHAR (255) NOT NULL)''')



def downgrade():
    pass
