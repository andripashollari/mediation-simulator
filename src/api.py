from flask import Flask, jsonify
from src.db_config import get_db_connection

app = Flask(__name__)

@app.route('/stats/costs', methods=['GET'])
def cost_stats():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT zone, COUNT(*) AS total_calls, AVG(cost) AS avg_cost
            FROM billing_feed
            GROUP BY zone
        """)
        rows = cur.fetchall()

        data = [
            {'zone': row[0], 'total_calls': row[1], 'avg_cost': float(row[2])}
            for row in rows
        ]
        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        cur.close()
        conn.close()


@app.route('/stats/summary', methods=['GET'])
def summary_stats():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM cdr_raw")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM processing_logs WHERE status = 'success'")
        success = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM processing_logs WHERE status = 'failed'")
        failed = cur.fetchone()[0]

        response = {
            'total_cdrs': total,
            'processed_successfully': success,
            'failed_processing': failed
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
