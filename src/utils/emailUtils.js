import nodemailer from 'nodemailer';

if (!process.env.SMTP_HOST || !process.env.SMTP_PORT || !process.env.SMTP_EMAIL || !process.env.SMTP_PASSWORDS) {
    throw new Error('Missing required SMTP configuration in environment variables');
}

const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT),
    secure: false,
    auth: {
        user: process.env.SMTP_EMAIL,
        pass: process.env.SMTP_PASSWORDS,
    },
    tls: {
        rejectUnauthorized: false
    },
    connectionTimeout: 30000,
    greetingTimeout: 30000,
    socketTimeout: 30000,
    debug: true,
    logger: true
});

const commonMailOptions = {
    from: `"Exellius" <${process.env.SMTP_EMAIL}>`,
    headers: {
        'X-Mailer': 'Nodemailer',
        'X-Priority': '3',
    }
};

export async function sendEmail(params) {
    const { to, subject, text, html, attachments } = params;

    const mailOptions = {
        ...commonMailOptions,
        to,
        subject,
        text: text || '',
        html: html || text || '',
        attachments: attachments || []
    };

    try {
        const info = await transporter.sendMail(mailOptions);
        console.log(`Email sent to ${to}`, info.messageId);
        return info;
    } catch (error) {
        console.error('SMTP Error:', {
            code: error.code,
            message: error.message,
            stack: error.stack
        });
        throw error;
    }
}

export async function sendOtpEmail(email, options = {}) {
    return sendEmail({
        to: email,
        ...options
    });
}

export async function sendCSVEmail(toEmail, csvData, options = {}) {
    const defaultOptions = {
        subject: 'Exported Leads Data',
        text: 'Please find the attached CSV file containing the exported leads data.',
        html: `
            <div style="font-family: Arial, sans-serif; line-height: 1.6;">
                <p>Please find the attached CSV file containing the exported leads data.</p>
                <p>If you have any questions about this data, please reply to this email.</p>
            </div>
        `,
        attachments: [{
            filename: options.fileName || 'leads_export.csv',
            content: csvData,
            contentType: 'text/csv'
        }]
    };

    return sendEmail({
        to: toEmail,
        ...defaultOptions,
        ...options
    });
}