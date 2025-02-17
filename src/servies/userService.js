import pool from '../config/db.js';


const deductCredits = async (userId, creditsToDeduct) => {
    try {
        const query = `
            UPDATE users
            SET credits = credits - $1
            WHERE id = $2 AND credits >= $1
            RETURNING credits;
        `;
        const { rows } = await pool.query(query, [creditsToDeduct, userId]);

        if (rows.length === 0) {
            return { success: false, message: 'Insufficient credits' };
        }

        return { success: true, remainingCredits: rows[0].credits };
    } catch (error) {
        console.error('Error deducting credits:', error);
        throw error;
    }
};


export { deductCredits }