from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class ClockLog(db.Model):
    __tablename__ = "clock_logs"
    id = db.Column(db.Integer, primary_key=True)
    crew_name = db.Column(db.String(80), nullable=False)
    action = db.Column(db.String(20), nullable=False)   # 'clock_in' or 'clock_out'
    ts = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    job_name = db.Column(db.String(120))
