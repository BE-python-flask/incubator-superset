"""add table cas

Revision ID: 2_0_1
Revises: None
Create Date: 2018-08-08
"""

# revision identifiers, used by Alembic.
revision = '2_0_1'
down_revision = '2_0_0'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('cas',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('type', sa.String(length=32), nullable=False),
        sa.Column('k', sa.String(length=256), nullable=False),
        sa.Column('v', sa.String(length=256), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('cas')
