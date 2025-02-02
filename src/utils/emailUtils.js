import nodemailer from 'nodemailer';

export async function sendOtpEmail(email, otp) {
    const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: {
            user: 'youremail@gmail.com',  // replace with your email
            pass: 'yourpassword',         // replace with your email password or app password
        },
    });

    const mailOptions = {
        from: 'youremail@gmail.com',
        to: email,
        subject: 'Your OTP for Verification',
        text: `Your OTP for verification is: ${otp}`,
    };

    try {
        await transporter.sendMail(mailOptions);
        console.log(`OTP sent to ${email}`);
    } catch (error) {
        console.error('Error sending OTP email:', error);
    }
}
