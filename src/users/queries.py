INSERT_CERTIFICATION = """

INSERT INTO users.certification (id , email, certification_number, expired_at)
VALUES (:id, :email, :certification_number, :expired_at)

"""

GET_CERTIFICATION = """

SELECT certification_number FROM users.certification
WHERE id = :id and expired_at > :now

"""

INSERT_REQUEST_ACCESS = """

INSERT INTO users.request_access (user_id, admin_email, request_mg)
VALUES (:user_id, :admin_email, :request_mg)

"""
